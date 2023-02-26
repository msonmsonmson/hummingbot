from decimal import Decimal

import pandas as pd

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import OrderFilledEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class XEMMTriangularArbitrage(ScriptStrategyBase):
    """
    BotCamp Cohort: Sept 2022
    Design Template: https://hummingbot-foundation.notion.site/Simple-XEMM-Example-f08cf7546ea94a44b389672fd21bb9ad
    Video: https://www.loom.com/share/ca08fe7bc3d14ba68ae704305ac78a3a
    Description:
    A simplified version of Hummingbot cross-exchange market making strategy, this bot makes a market on
    the maker pair and hedges any filled trades in the taker pair. If the spread (difference between maker order price
    and taker hedge price) dips below min_spread, the bot refreshes the order
    """

    maker_exchange = "kucoin_paper_trade"
    maker_pair = "CKB-BTC"
    taker_exchange = "binance_paper_trade"
    taker_pair1 = "CKB-USDT"
    taker_pair2 = "BTC-USDT"

    order_amount = Decimal(.0001)                 # amount for each order
    spread_bps = 10                     # bot places maker orders at this spread to taker price
    min_spread_bps = 0                  # bot refreshes order if spread is lower than min-spread
    slippage_buffer_spread_bps = 100    # buffer applied to limit taker hedging trades on taker exchange
    max_order_age = 120                 # bot refreshes orders after this age
    min_profitability = 0.001
    # profit_amount = (1 + min_profitability)
    # profit_amount = order_amount * (1 + min_profitability)
    profit_amount = 0.0001 * (1 + 0.001)
    profit_amount_sell = 0.0001 * (1 - 0.001)

    markets = {maker_exchange: {maker_pair}, taker_exchange: {taker_pair1, taker_pair2}}

    buy_order_placed = False
    sell_order_placed = False

    def on_tick(self):
        taker1_order_book = self.connectors[self.taker_exchange].get_order_book(self.taker_pair1)

        taker2_buy_exchanged_amount = self.connectors[self.taker_exchange].get_quote_volume_for_base_amount(self.taker_pair2, 0, self.profit_amount).result_volume
        taker1_sell_exchanged_amount = self.get_base_amount_for_quote_volume(taker1_order_book.bid_entries(), taker2_buy_exchanged_amount)
        maker_bid_price = self.order_amount / taker1_sell_exchanged_amount

        taker2_sell_exchanged_amount = self.connectors[self.taker_exchange].get_quote_volume_for_base_amount(self.taker_pair2, 1, self.profit_amount_sell).result_volume
        taker1_buy_exchanged_amount = self.get_base_amount_for_quote_volume(taker1_order_book.ask_entries(), taker2_sell_exchanged_amount)
        maker_ask_price = self.order_amount / taker1_buy_exchanged_amount


        # self.logger().info(f"Best bid: {self.connectors[self.maker_exchange].get_price('QUICK-BTC', False)}")
        # self.logger().info(f"Best Offer: {self.connectors[self.maker_exchange].get_price('QUICK-BTC', True)}")
        # self.logger().info(f"Best bid: {self.connectors[self.taker_exchange].get_price('QUICK-USDT', False)}")
        # self.logger().info(f"Best Offer: {self.connectors[self.taker_exchange].get_price('BTC-USDT', False)}")

        if not self.buy_order_placed:
            # maker_buy_price = taker_sell_result.result_price * Decimal(1 - self.spread_bps / 10000)
            # buy_order_amount = min(self.order_amount, self.buy_hedging_budget())
            maker_buy_price = maker_bid_price
            buy_order_amount = self.order_amount
            buy_order = OrderCandidate(trading_pair=self.maker_pair, is_maker=True, order_type=OrderType.LIMIT, order_side=TradeType.BUY, amount=Decimal(buy_order_amount), price=maker_buy_price)
            buy_order_adjusted = self.connectors[self.maker_exchange].budget_checker.adjust_candidate(buy_order, all_or_none=False)
            self.buy(self.maker_exchange, self.maker_pair, buy_order_adjusted.amount, buy_order_adjusted.order_type, buy_order_adjusted.price)
            self.buy_order_placed = True

        if not self.sell_order_placed:
            # maker_sell_price = taker_buy_result.result_price * Decimal(1 + self.spread_bps / 10000)
            # sell_order_amount = min(self.order_amount, self.sell_hedging_budget())
            maker_sell_price = maker_ask_price
            sell_order_amount = self.order_amount
            sell_order = OrderCandidate(trading_pair=self.maker_pair, is_maker=True, order_type=OrderType.LIMIT, order_side=TradeType.SELL, amount=Decimal(sell_order_amount), price=maker_sell_price)
            sell_order_adjusted = self.connectors[self.maker_exchange].budget_checker.adjust_candidate(sell_order, all_or_none=False)
            self.sell(self.maker_exchange, self.maker_pair, sell_order_adjusted.amount, sell_order_adjusted.order_type, sell_order_adjusted.price)
            self.sell_order_placed = True

        for order in self.get_active_orders(connector_name=self.maker_exchange):
            cancel_timestamp = order.creation_timestamp / 1000000 + self.max_order_age
            if order.is_buy:
                # buy_cancel_threshold = taker_sell_result.result_price * Decimal(1 - self.min_spread_bps / 10000)
                # if order.price > buy_cancel_threshold or cancel_timestamp < self.current_timestamp:
                if cancel_timestamp < self.current_timestamp:
                    self.logger().info(f"Cancelling buy order: {order.client_order_id}")
                    self.cancel(self.maker_exchange, order.trading_pair, order.client_order_id)
                    self.buy_order_placed = False
            else:
                # sell_cancel_threshold = taker_buy_result.result_price * Decimal(1 + self.min_spread_bps / 10000)
                # if order.price < sell_cancel_threshold or cancel_timestamp < self.current_timestamp:
                if cancel_timestamp < self.current_timestamp:
                    self.logger().info(f"Cancelling sell order: {order.client_order_id}")
                    self.cancel(self.maker_exchange, order.trading_pair, order.client_order_id)
                    self.sell_order_placed = False
        return

    def buy_hedging_budget(self) -> Decimal:
        balance = self.connectors[self.taker_exchange].get_available_balance("ETH")
        return balance

    def sell_hedging_budget(self) -> Decimal:
        balance = self.connectors[self.taker_exchange].get_available_balance("USDT")
        taker_buy_result = self.connectors[self.taker_exchange].get_price_for_volume(self.taker_pair, True, self.order_amount)
        return balance / taker_buy_result.result_price

    def is_active_maker_order(self, event: OrderFilledEvent):
        """
        Helper function that checks if order is an active order on the maker exchange
        """
        for order in self.get_active_orders(connector_name=self.maker_exchange):
            if order.client_order_id == event.order_id:
                return True
        return False

    def get_base_amount_for_quote_volume(self, orderbook_entries, quote_volume) -> Decimal:
        """
        Calculates base amount that you get for the quote volume using the orderbook entries
        """
        cumulative_volume = 0.
        cumulative_base_amount = 0.
        quote_volume = float(quote_volume)

        for order_book_row in orderbook_entries:
            row_amount = order_book_row.amount
            row_price = order_book_row.price
            row_volume = row_amount * row_price
            if row_volume + cumulative_volume >= quote_volume:
                row_volume = quote_volume - cumulative_volume
                row_amount = row_volume / row_price
            cumulative_volume += row_volume
            cumulative_base_amount += row_amount
            if cumulative_volume >= quote_volume:
                break

        return Decimal(cumulative_base_amount)

    def did_fill_order(self, event: OrderFilledEvent):

        mid_price = self.connectors[self.maker_exchange].get_mid_price(self.maker_pair)
        if event.trade_type == TradeType.BUY and self.is_active_maker_order(event):
            taker1_sell_result = self.connectors[self.taker_exchange].get_price_for_volume(self.taker_pair1, False, self.order_amount)
            taker1_order_book = self.connectors[self.taker_exchange].get_order_book(self.taker_pair1)
            taker1_sell_amount = self.get_base_amount_for_quote_volume(taker1_order_book.bid_entries(), event.amount)
            taker2_buy_result = self.connectors[self.taker_exchange].get_price_for_volume(self.taker_pair2, True, taker1_sell_amount)
            taker2_buy_amount = self.connectors[self.taker_exchange].get_quote_volume_for_base_amount(self.taker_pair2, 0, taker1_sell_amount).result_volume

            sell_price_with_slippage = taker1_sell_result.result_price * Decimal(1 - self.slippage_buffer_spread_bps / 10000)
            self.logger().info(f"Filled maker buy order with price: {event.price}")
            # sell_spread_bps = (taker1_sell_result.result_price - event.price) / mid_price * 10000
            self.logger().info(f"Sending taker sell order at price: {taker1_sell_result.result_price} "
                               # f"spread: {int(sell_spread_bps)} bps"
                               )
            sell_order = OrderCandidate(trading_pair=self.taker_pair1, is_maker=False, order_type=OrderType.LIMIT, order_side=TradeType.SELL, amount=Decimal(taker1_sell_amount), price=sell_price_with_slippage)
            sell_order_adjusted = self.connectors[self.taker_exchange].budget_checker.adjust_candidate(sell_order, all_or_none=False)
            self.sell(self.taker_exchange, self.taker_pair1, sell_order_adjusted.amount, sell_order_adjusted.order_type, sell_order_adjusted.price)


            buy_price_with_slippage = taker2_buy_result.result_price * Decimal(1 - self.slippage_buffer_spread_bps / 10000)
            # self.logger().info(f"Filled maker buy order with price: {event.price}")
            # buy_spread_bps = (taker2_buy_result.result_price - event.price) / mid_price * 10000
            self.logger().info(f"Sending taker buy order at price: {taker2_buy_result.result_price} "
                               # f"spread: {int(sell_spread_bps)} bps"
                               )
            buy_order = OrderCandidate(trading_pair=self.taker_pair2, is_maker=False, order_type=OrderType.LIMIT, order_side=TradeType.BUY, amount=Decimal(taker2_buy_amount), price=buy_price_with_slippage)
            buy_order_adjusted = self.connectors[self.taker_exchange].budget_checker.adjust_candidate(buy_order, all_or_none=False)
            self.buy(self.taker_exchange, self.taker_pair2, buy_order_adjusted.amount, buy_order_adjusted.order_type, buy_order_adjusted.price)
            self.buy_order_placed = False

        else:
            if event.trade_type == TradeType.SELL and self.is_active_maker_order(event):
                taker1_buy_result = self.connectors[self.taker_exchange].get_price_for_volume(self.taker_pair1, True, self.order_amount)
                taker1_order_book = self.connectors[self.taker_exchange].get_order_book(self.taker_pair1)
                taker1_buy_amount = self.get_base_amount_for_quote_volume(taker1_order_book.ask_entries(), event.amount)
                taker2_sell_result = self.connectors[self.taker_exchange].get_price_for_volume(self.taker_pair2, False, taker1_buy_amount)
                taker2_sell_amount = self.connectors[self.taker_exchange].get_quote_volume_for_base_amount(self.taker_pair2, 0, taker1_buy_amount).result_volume

                buy_price_with_slippage = taker1_buy_result.result_price * Decimal(1 - self.slippage_buffer_spread_bps / 10000)
                self.logger().info(f"Filled maker sell order with price: {event.price}")
                # sell_spread_bps = (taker1_sell_result.result_price - event.price) / mid_price * 10000
                self.logger().info(f"Sending taker buy order at price: {taker1_buy_result.result_price} "
                                   # f"spread: {int(sell_spread_bps)} bps"
                                   )
                buy_order = OrderCandidate(trading_pair=self.taker_pair1, is_maker=False, order_type=OrderType.LIMIT, order_side=TradeType.SELL, amount=Decimal(taker1_buy_amount), price=buy_price_with_slippage)
                buy_order_adjusted = self.connectors[self.taker_exchange].budget_checker.adjust_candidate(buy_order, all_or_none=False)
                self.buy(self.taker_exchange, self.taker_pair1, buy_order_adjusted.amount, buy_order_adjusted.order_type, buy_order_adjusted.price)

                sell_price_with_slippage = taker2_sell_result.result_price * Decimal(1 - self.slippage_buffer_spread_bps / 10000)
                # self.logger().info(f"Filled maker buy order with price: {event.price}")
                # buy_spread_bps = (taker2_buy_result.result_price - event.price) / mid_price * 10000
                self.logger().info(f"Sending taker sell order at price: {taker2_sell_result.result_price} "
                                   # f"spread: {int(sell_spread_bps)} bps"
                                   )
                sell_order = OrderCandidate(trading_pair=self.taker_pair2, is_maker=False, order_type=OrderType.LIMIT, order_side=TradeType.BUY, amount=Decimal(taker2_sell_amount), price=sell_price_with_slippage)
                sell_order_adjusted = self.connectors[self.taker_exchange].budget_checker.adjust_candidate(buy_order, all_or_none=False)
                self.sell(self.taker_exchange, self.taker_pair2, sell_order_adjusted.amount, sell_order_adjusted.order_type, sell_order_adjusted.price)
                self.sell_order_placed = False


    def exchanges_df(self) -> pd.DataFrame:
            """
            Return a custom data frame of prices on maker vs taker exchanges for display purposes
            """
            mid_price = self.connectors[self.maker_exchange].get_mid_price(self.maker_pair)
            maker_buy_result = self.connectors[self.maker_exchange].get_price_for_volume(self.taker_pair, True, self.order_amount)
            maker_sell_result = self.connectors[self.maker_exchange].get_price_for_volume(self.taker_pair, False, self.order_amount)
            taker_buy_result = self.connectors[self.taker_exchange].get_price_for_volume(self.taker_pair, True, self.order_amount)
            taker_sell_result = self.connectors[self.taker_exchange].get_price_for_volume(self.taker_pair, False, self.order_amount)
            maker_buy_spread_bps = (maker_buy_result.result_price - taker_buy_result.result_price) / mid_price * 10000
            maker_sell_spread_bps = (taker_sell_result.result_price - maker_sell_result.result_price) / mid_price * 10000
            columns = ["Exchange", "Market", "Mid Price", "Buy Price", "Sell Price", "Buy Spread", "Sell Spread"]
            data = []
            data.append([
                self.maker_exchange,
                self.maker_pair,
                float(self.connectors[self.maker_exchange].get_mid_price(self.maker_pair)),
                float(maker_buy_result.result_price),
                float(maker_sell_result.result_price),
                int(maker_buy_spread_bps),
                int(maker_sell_spread_bps)
            ])
            data.append([
                self.taker_exchange,
                self.taker_pair,
                float(self.connectors[self.taker_exchange].get_mid_price(self.maker_pair)),
                float(taker_buy_result.result_price),
                float(taker_sell_result.result_price),
                int(-maker_buy_spread_bps),
                int(-maker_sell_spread_bps)
            ])
            df = pd.DataFrame(data=data, columns=columns)
            return df

    def active_orders_df(self) -> pd.DataFrame:
        """
        Returns a custom data frame of all active maker orders for display purposes
        """
        columns = ["Exchange", "Market", "Side", "Price", "Amount", "Spread Mid", "Spread Cancel", "Age"]
        data = []
        mid_price = self.connectors[self.maker_exchange].get_mid_price(self.maker_pair)
        taker_buy_result = self.connectors[self.taker_exchange].get_price_for_volume(self.taker_pair, True, self.order_amount)
        taker_sell_result = self.connectors[self.taker_exchange].get_price_for_volume(self.taker_pair, False, self.order_amount)
        buy_cancel_threshold = taker_sell_result.result_price * Decimal(1 - self.min_spread_bps / 10000)
        sell_cancel_threshold = taker_buy_result.result_price * Decimal(1 + self.min_spread_bps / 10000)
        for connector_name, connector in self.connectors.items():
            for order in self.get_active_orders(connector_name):
                age_txt = "n/a" if order.age() <= 0. else pd.Timestamp(order.age(), unit='s').strftime('%H:%M:%S')
                spread_mid_bps = (mid_price - order.price) / mid_price * 10000 if order.is_buy else (order.price - mid_price) / mid_price * 10000
                spread_cancel_bps = (buy_cancel_threshold - order.price) / buy_cancel_threshold * 10000 if order.is_buy else (order.price - sell_cancel_threshold) / sell_cancel_threshold * 10000
                data.append([
                    self.maker_exchange,
                    order.trading_pair,
                    "buy" if order.is_buy else "sell",
                    float(order.price),
                    float(order.quantity),
                    int(spread_mid_bps),
                    int(spread_cancel_bps),
                    age_txt
                ])
        if not data:
            raise ValueError
        df = pd.DataFrame(data=data, columns=columns)
        df.sort_values(by=["Market", "Side"], inplace=True)
        return df

    def format_status(self) -> str:
        """
        Returns status of the current strategy on user balances and current active orders. This function is called
        when status command is issued. Override this function to create custom status display output.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []

        balance_df = self.get_balance_df()
        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])

        exchanges_df = self.exchanges_df()
        lines.extend(["", "  Exchanges:"] + ["    " + line for line in exchanges_df.to_string(index=False).split("\n")])

        try:
            orders_df = self.active_orders_df()
            lines.extend(["", "  Active Orders:"] + ["    " + line for line in orders_df.to_string(index=False).split("\n")])
        except ValueError:
            lines.extend(["", "  No active maker orders."])

        return "\n".join(lines)
