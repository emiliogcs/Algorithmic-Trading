#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 15 13:36:26 2021

@author: emiliogonzalez
"""

import alpaca_trade_api as tradeapi
import requests
import time
import threading
from ta.trend import macd
import numpy as np
from datetime import datetime, timedelta
from pytz import timezone



#nest_asyncio.apply()
#__import__('IPython').embed()



# Replace these with your API connection info from the dashboard
API_KEY = 'PKUVM6TOFEBU7T5EXLMQ'
API_SECRET = 'H4ovbu6ynLGUwJ6SCDeSaC03qL2gb1fJv9JtbECG'
APCA_API_BASE_URL = "https://paper-api.alpaca.markets"

class MomentumA:
    def __init__(self):
    
        self.api = tradeapi.REST(API_KEY, API_SECRET, APCA_API_BASE_URL, 'v2')
        self.session = requests.session()
        
        # We only consider stocks with per-share prices inside this range
        self.min_share_price = 2.0
        self.max_share_price = 13.0
        # Minimum previous-day dollar volume for a stock we might consider
        self.min_last_dv = 500000
        # Stop limit to default to
        self.default_stop = .95
        # How much of our portfolio to allocate to any one position
        self.risk = 0.001
    
    def get_1000m_history_data(self):
        print('Getting historical data...')
        minute_history = {}
        c = 0
        today = datetime.today()
        yesterday = datetime.today() - timedelta(days=1)
        for symbol in self.symbols:
            minute_history[symbol] = self.api.polygon.historic_agg_v2(
                 timespan="minute", symbol=symbol, limit=1000,
                 multiplier=1, _from=yesterday.strftime("%Y-%m-%d"), 
                 to=today.strftime("%Y-%m-%d")).df
            c += 1
            print('{}/{}'.format(c, len(self.symbols)))
        print('Success.')
        return minute_history
    
    
    def get_tickers(self):
        print('Getting current ticker data...')
        tickers = self.api.polygon.all_tickers()
        print('Success.')
        assets = self.api.list_assets()
        symbols = [asset.symbol for asset in assets if asset.tradable]
        return [ticker for ticker in tickers if (
            ticker.ticker in symbols and
            ticker.lastTrade['p'] >= self.min_share_price and
            ticker.lastTrade['p'] <= self.max_share_price and
            ticker.prevDay['v'] * ticker.lastTrade['p'] > self.min_last_dv and
            ticker.todaysChangePerc >= 3.5
        )]
    
    
    def find_stop(current_value, minute_history, now, self):
        series = minute_history['low'][-100:] \
                    .dropna().resample('5min').min()
        series = series[now.floor('1D'):]
        diff = np.diff(series.values)
        low_index = np.where((diff[:-1] <= 0) & (diff[1:] > 0))[0] + 1
        if len(low_index) > 0:
            return series[low_index[-1]] - 0.01
        return current_value * self.default_stop
    
    
    def run(self):
        # Establish streaming connection
        self.conn = tradeapi.StreamConn(base_url=APCA_API_BASE_URL, key_id=API_KEY, secret_key=API_SECRET)
        

        # Wait for market to open.
        print("Waiting for market to open...")
        tAMO = threading.Thread(target=self.awaitMarketOpen)
        tAMO.start()
        tAMO.join()
        print("Market opened.")
    
        # Update initial state with information from tickers
        volume_today = {}
        prev_closes = {}
        for ticker in self.get_tickers():
            symbol = ticker.ticker
            prev_closes[symbol] = ticker.prevDay['c']
            volume_today[symbol] = ticker.day['v']
    
        self.symbols = [ticker.ticker for ticker in self.get_tickers()]
        print('Tracking {} symbols.'.format(len(self.symbols)))
        minute_history = self.get_1000m_history_data()
    
        portfolio_value = float(self.api.get_account().portfolio_value)
    
        open_orders = {}
        positions = {}
    
        # Cancel any existing open orders on watched symbols
        existing_orders = self.api.list_orders(limit=500)
        for order in existing_orders:
            if order.symbol in self.symbols:
                self.api.cancel_order(order.id)
    
        stop_prices = {}
        latest_cost_basis = {}
    
        # Track any positions bought during previous executions
        existing_positions = self.api.list_positions()
        for position in existing_positions:
            if position.symbol in self.symbols:
                positions[position.symbol] = float(position.qty)
                # Recalculate cost basis and stop price
                latest_cost_basis[position.symbol] = float(position.cost_basis)
                stop_prices[position.symbol] = (
                    float(position.cost_basis) * self.default_stop
                )
    
        # Keep track of what we're buying/selling
        target_prices = {}
        partial_fills = {}
    
        # Use trade updates to keep track of our portfolio
        @self.conn.on(r'trade_update')
        async def handle_trade_update(conn, channel, data):
            symbol = data.order['symbol']
            last_order = open_orders.get(symbol)
            if last_order is not None:
                event = data.event
                if event == 'partial_fill':
                    qty = int(data.order['filled_qty'])
                    if data.order['side'] == 'sell':
                        qty = qty * -1
                    positions[symbol] = (
                        positions.get(symbol, 0) - partial_fills.get(symbol, 0)
                    )
                    partial_fills[symbol] = qty
                    positions[symbol] += qty
                    open_orders[symbol] = data.order
                elif event == 'fill':
                    qty = int(data.order['filled_qty'])
                    if data.order['side'] == 'sell':
                        qty = qty * -1
                    positions[symbol] = (
                        positions.get(symbol, 0) - partial_fills.get(symbol, 0)
                    )
                    partial_fills[symbol] = 0
                    positions[symbol] += qty
                    open_orders[symbol] = None
                elif event == 'canceled' or event == 'rejected':
                    partial_fills[symbol] = 0
                    open_orders[symbol] = None
    
        @self.conn.on(r'A$')
        async def handle_second_bar(conn, channel, data):
            symbol = data.symbol
    
            # First, aggregate 1s bars for up-to-date MACD calculations
            ts = data.start
            ts -= timedelta(seconds=ts.second, microseconds=ts.microsecond)
            try:
                current = minute_history[data.symbol].loc[ts]
            except KeyError:
                current = None
            new_data = []
            if current is None:
                new_data = [
                    data.open,
                    data.high,
                    data.low,
                    data.close,
                    data.volume
                ]
            else:
                new_data = [
                    current.open,
                    data.high if data.high > current.high else current.high,
                    data.low if data.low < current.low else current.low,
                    data.close,
                    current.volume + data.volume
                ]
            minute_history[symbol].loc[ts] = new_data
    
            # Next, check for existing orders for the stock
            existing_order = open_orders.get(symbol)
            if existing_order is not None:
                # Make sure the order's not too old
                submission_ts = existing_order.submitted_at.astimezone(
                    timezone('America/New_York')
                )
                order_lifetime = ts - submission_ts
                if order_lifetime.seconds // 60 > 1:
                    # Cancel it so we can try again for a fill
                    self.api.cancel_order(existing_order.id)
                return
    
            # Now we check to see if it might be time to buy or sell
            since_market_open = ts - self.market_open_dt
            until_market_close = self.market_close_dt - ts
            if (
                since_market_open.seconds // 60 > 15 and
                since_market_open.seconds // 60 < 60
            ):
                # Check for buy signals
    
                # See if we've already bought in first
                position = positions.get(symbol, 0)
                if position > 0:
                    return
    
                # See how high the price went during the first 15 minutes
                lbound = self.market_open_dt
                ubound = lbound + timedelta(minutes=15)
                high_15m = 0
                try:
                    high_15m = minute_history[symbol][lbound:ubound]['high'].max()
                except Exception as e:
                    # Because we're aggregating on the fly, sometimes the datetime
                    # index can get messy until it's healed by the minute bars
                    return
    
                # Get the change since yesterday's market close
                daily_pct_change = (
                    (data.close - prev_closes[symbol]) / prev_closes[symbol]
                )
                if (
                    daily_pct_change > .04 and
                    data.close > high_15m and
                    volume_today[symbol] > 30000
                ):
                    # check for a positive, increasing MACD
                    hist = macd(
                        minute_history[symbol]['close'].dropna(),
                        n_fast=12,
                        n_slow=26
                    )
                    if (
                        hist[-1] < 0 or
                        not (hist[-3] < hist[-2] < hist[-1])
                    ):
                        return
                    hist = macd(
                        minute_history[symbol]['close'].dropna(),
                        n_fast=40,
                        n_slow=60
                    )
                    if hist[-1] < 0 or np.diff(hist)[-1] < 0:
                        return
    
                    # Stock has passed all checks; figure out how much to buy
                    stop_price = self.find_stop(
                        data.close, minute_history[symbol], ts
                    )
                    stop_prices[symbol] = stop_price
                    target_prices[symbol] = data.close + (
                        (data.close - stop_price) * 3
                    )
                    shares_to_buy = portfolio_value * self.risk // (
                        data.close - stop_price
                    )
                    if shares_to_buy == 0:
                        shares_to_buy = 1
                    shares_to_buy -= positions.get(symbol, 0)
                    if shares_to_buy <= 0:
                        return
    
                    print('Submitting buy for {} shares of {} at {}'.format(
                        shares_to_buy, symbol, data.close
                    ))
                    try:
                        o = self.api.submit_order(
                            symbol=symbol, qty=str(shares_to_buy), side='buy',
                            type='limit', time_in_force='day',
                            limit_price=str(data.close)
                        )
                        open_orders[symbol] = o
                        latest_cost_basis[symbol] = data.close
                    except Exception as e:
                        print(e)
                    return
            if(
                since_market_open.seconds // 60 >= 24 and
                until_market_close.seconds // 60 > 15
            ):
                # Check for liquidation signals
    
                # We can't liquidate if there's no position
                position = positions.get(symbol, 0)
                if position == 0:
                    return
    
                # Sell for a loss if it's fallen below our stop price
                # Sell for a loss if it's below our cost basis and MACD < 0
                # Sell for a profit if it's above our target price
                hist = macd(
                    minute_history[symbol]['close'].dropna(),
                    n_fast=13,
                    n_slow=21
                )
                if (
                    data.close <= stop_prices[symbol] or
                    (data.close >= target_prices[symbol] and hist[-1] <= 0) or
                    (data.close <= latest_cost_basis[symbol] and hist[-1] <= 0)
                ):
                    print('Submitting sell for {} shares of {} at {}'.format(
                        position, symbol, data.close
                    ))
                    try:
                        o = self.api.submit_order(
                            symbol=symbol, qty=str(position), side='sell',
                            type='limit', time_in_force='day',
                            limit_price=str(data.close)
                        )
                        open_orders[symbol] = o
                        latest_cost_basis[symbol] = data.close
                    except Exception as e:
                        print(e)
                return
            elif (
                until_market_close.seconds // 60 <= 15
            ):
                # Liquidate remaining positions on watched symbols at market
                try:
                    position = self.api.get_position(symbol)
                except Exception as e:
                    # Exception here indicates that we have no position
                    return
                print('Trading over, liquidating remaining position in {}'.format(
                    symbol)
                )
                self.api.submit_order(
                    symbol=symbol, qty=position.qty, side='sell',
                    type='market', time_in_force='day'
                )
                self.symbols.remove(symbol)
                if len(self.symbols) <= 0:
                    conn.close()
                conn.deregister([
                    'A.{}'.format(symbol),
                    'AM.{}'.format(symbol)
                ])
    
        # Replace aggregated 1s bars with incoming 1m bars
        @self.conn.on(r'AM$')
        async def handle_minute_bar(conn, channel, data):
            ts = data.start
            ts -= timedelta(microseconds=ts.microsecond)
            minute_history[data.symbol].loc[ts] = [
                data.open,
                data.high,
                data.low,
                data.close,
                data.volume
            ]
            volume_today[data.symbol] += data.volume
    
        self.channels = ['trade_updates']
        for symbol in self.symbols:
            symbol_channels = ['A.{}'.format(symbol), 'AM.{}'.format(symbol)]
            self.channels += symbol_channels
        print('Watching {} symbols.'.format(len(self.symbols)))
        self.run_ws()
        
    # Handle failed websocket connections by reconnecting
    def run_ws(self):
        try:
            self.conn.run(self.channels)
        except Exception as e:
            print(e)
            self.conn.close()
            self.run_ws(self.conn,self.channels)
        
        
    # Wait for market to open.
    def awaitMarketOpen(self):
        isOpen = self.api.get_clock().is_open
        while(not isOpen):
          clock = self.api.get_clock()
          openingTime = clock.next_open.replace(tzinfo=datetime.timezone.utc).timestamp()
          currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
          timeToOpen = int((openingTime - currTime) / 60)
          print(str(timeToOpen) + " minutes til market open.")
          time.sleep(60)
          isOpen = self.api.get_clock().is_open
    
    
# Run the MomentumA class

ls = MomentumA()
ls.run()
 
    
        

