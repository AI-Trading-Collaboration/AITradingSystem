from AlgorithmImports import *
from datetime import datetime, timedelta
import json, math

# TRADING-2542I: one owner-authorized QC DATA_RESEARCH backtest; no live/broker/export path.
POLICY_SHA="0d48cc3080b2ce3730c1f97c0a8ae77969617b22eb60ccc5c32ca586bce548b1"
PACKAGE_SHA="7cb8807c5938be5453e49c392e3173aca38e10643c643c28b335914196eda494"
TRANSITION_SHA="9933d74d5ff4fc23bcb8ef70f6e889cc6544098deeb427ae80d165dddd35c2ad"
TRANSITIONS=(('2021-02-23', False), ('2021-04-07', True), ('2021-04-08', False), ('2021-04-09', True), ('2021-04-12', False), ('2021-04-13', True), ('2021-04-15', False), ('2021-04-19', True), ('2021-04-21', False), ('2021-04-22', True), ('2021-05-03', False), ('2021-06-11', True), ('2021-07-20', False), ('2021-07-21', True), ('2021-07-28', False), ('2021-07-30', True), ('2021-08-19', False), ('2021-08-23', True), ('2021-09-21', False), ('2021-10-27', True), ('2021-11-09', False), ('2021-11-19', True), ('2021-11-24', False), ('2023-03-16', True), ('2023-08-10', False), ('2023-08-31', True), ('2023-09-08', False), ('2023-09-12', True), ('2023-09-13', False), ('2023-09-14', True), ('2023-09-18', False), ('2023-11-13', True), ('2023-11-14', False), ('2023-11-15', True), ('2023-11-22', False), ('2023-12-11', True), ('2024-02-22', False), ('2024-02-23', True), ('2024-03-11', False), ('2024-03-14', True), ('2024-03-15', False), ('2024-03-20', True), ('2024-03-27', False), ('2024-03-28', True), ('2024-04-03', False), ('2024-04-04', True), ('2024-04-05', False), ('2024-04-09', True), ('2024-04-11', False), ('2024-04-12', True), ('2024-04-15', False), ('2024-05-16', True), ('2024-05-17', False), ('2024-05-21', True), ('2024-05-23', False), ('2024-05-28', True), ('2024-05-30', False), ('2024-06-06', True), ('2024-07-18', False), ('2024-10-15', True), ('2024-10-16', False), ('2024-10-21', True), ('2024-10-24', False), ('2024-10-29', True), ('2024-11-01', False), ('2024-11-08', True), ('2024-11-11', False), ('2024-12-02', True), ('2024-12-10', False), ('2024-12-12', True), ('2024-12-19', False), ('2025-02-18', True), ('2025-02-19', False), ('2025-06-26', True), ('2025-08-04', False), ('2025-08-05', True), ('2025-08-12', False), ('2025-08-13', True), ('2025-08-18', False), ('2025-08-19', True), ('2025-08-20', False), ('2025-09-16', True), ('2025-10-13', False))
START_CASH=100000.0

class PerContractFee(FeeModel):
    def get_order_fee(self, p):
        return OrderFee(CashAmount(abs(p.order.quantity)*0.65,"USD"))

class AdverseLimitFill(ImmediateFillModel):
    def limit_fill(self, asset, order):
        event=super().limit_fill(asset,order)
        if event.status==OrderStatus.FILLED:
            if order.quantity>0: event.fill_price=min(order.limit_price,event.fill_price+0.01)
            else: event.fill_price=max(order.limit_price,event.fill_price-0.01)
        return event

class QQQOptionsExactSignalImplementationRetest(QCAlgorithm):
    def initialize(self):
        self.set_start_date(2021,2,22); self.set_end_date(2025,12,2)
        self.set_cash(START_CASH); self.set_time_zone(TimeZones.NEW_YORK)
        self.set_brokerage_model(DefaultBrokerageModel(AccountType.CASH))
        self.universe_settings.asynchronous=False
        self._qqq=self.add_equity("QQQ",Resolution.MINUTE,fill_forward=False,
            data_normalization_mode=DataNormalizationMode.RAW).symbol
        option=self.add_option("QQQ",Resolution.MINUTE,fill_forward=False)
        option.set_filter(lambda u:u.include_weeklys().calls_only().expiration(30,45).contracts(self._universe))
        self._option=option.symbol; self._prior={}; self._ticket=None; self._pending=None
        self._open=None; self._selected_day=None; self._blocked_day=None; self._invalid=None
        self._sessions=set(); self._long_sessions=0; self._no_candidate=0; self._cancels=0
        self._submissions=0; self._fills=0; self._entries=0; self._exits=0
        self._cmp_active=False; self._cmp_factor=1.0; self._cmp_entry=None
        self._cmp_state=False; self._cmp_day=None; self._last_qqq_bid=None; self._last_qqq_bid_time=None
        self._last_open_bid=None; self._last_open_bid_time=None
        self._signal_day=None; self._signal_value=False

    def _fail(self, reason):
        if self._invalid is None: self._invalid=reason; self.quit(reason)

    @staticmethod
    def _finite(value):
        try: value=float(value)
        except (TypeError,ValueError): return None
        return value if math.isfinite(value) else None

    def _signal(self):
        day=self.time.date()
        if self._signal_day==day: return self._signal_value
        key=day.isoformat(); active=False
        for effective,value in TRANSITIONS:
            if effective>key: break
            active=value
        self._signal_day=day; self._signal_value=active
        return self._signal_value

    def _universe(self, contracts):
        selected=[]; prior={}
        for item in contracts:
            try:
                delta=abs(float(item.greeks.delta)); oi=int(item.open_interest); symbol=item.symbol
            except Exception: continue
            if math.isfinite(delta) and 0.45<=delta<=0.60 and oi>=100:
                selected.append(symbol); prior[symbol]=(delta,oi)
        self._prior=prior
        return selected

    def _quote(self,data,symbol):
        qb=data.quote_bars.get(symbol)
        if qb is None or qb.bid is None or qb.ask is None: return None
        bid=self._finite(qb.bid.close); ask=self._finite(qb.ask.close)
        if bid is None or ask is None or bid<0 or ask<=0 or ask<bid: return None
        end=getattr(qb,"end_time",self.time)
        age=(self.time-end).total_seconds()
        if age<0 or age>60: return None
        return bid,ask

    def _qqq_quote(self,data):
        quote=self._quote(data,self._qqq)
        if quote is not None: self._last_qqq_bid=quote[0]; self._last_qqq_bid_time=self.time
        return quote

    def _comparator(self,data,desired):
        if self._cmp_day==self.time.date() or self.time.hour<9 or (self.time.hour==9 and self.time.minute<32): return
        quote=self._qqq_quote(data)
        if quote is None: return
        self._cmp_day=self.time.date()
        if desired and not self._cmp_state:
            self._cmp_active=True; self._cmp_entry=quote[1]; self._cmp_state=True
        elif not desired and self._cmp_state:
            if self._cmp_entry is None or self._cmp_entry<=0: self._fail("COMPARATOR_ENTRY_INVALID"); return
            self._cmp_factor*=quote[0]/self._cmp_entry
            self._cmp_active=False; self._cmp_entry=None; self._cmp_state=False

    def _select(self,data):
        chain=data.option_chains.get(self._option)
        if chain is None: return None
        underlying=self._finite(self.securities[self._qqq].price)
        if underlying is None or underlying<=0: return None
        ranked=[]
        for contract in chain:
            symbol=contract.symbol; meta=self._prior.get(symbol)
            if meta is None: continue
            quote=self._quote(data,symbol)
            if quote is None: continue
            bid,ask=quote; mid=(bid+ask)/2.0
            if mid<=0 or (ask-bid)/mid>0.20: continue
            strike=float(symbol.id.strike_price); m=underlying/strike if strike>0 else 0
            expiry=symbol.id.date.date() if hasattr(symbol.id.date,"date") else symbol.id.date
            dte=(expiry-self.time.date()).days
            if not (30<=dte<=45 and 0.90<=m<=1.10): continue
            delta,oi=meta
            ranked.append(((abs(delta-0.50),abs(dte-35),(ask-bid)/mid,-oi,expiry,strike,str(symbol.id)),symbol,ask))
        return min(ranked,key=lambda x:x[0]) if ranked else None

    def _sessions_to_expiry(self,symbol):
        expiry=symbol.id.date.date() if hasattr(symbol.id.date,"date") else symbol.id.date
        hours=self.securities[self._qqq].exchange.hours; cursor=self.time; count=0
        for _ in range(15):
            nxt=hours.get_next_market_open(cursor,False)
            if nxt.date()>=expiry: break
            count+=1; cursor=nxt+timedelta(days=1)
        return count

    def _plan_exit(self):
        if self._pending is None and self._ticket is None and self._open is not None:
            self._pending=("EXIT",self._open,self.time); self._blocked_day=self.time.date()

    def _submit_pending(self,data):
        if self._pending is None or self.time<=self._pending[2]: return
        side,symbol,_=self._pending; quote=self._quote(data,symbol)
        if quote is None: return
        bid,ask=quote
        if side=="ENTRY":
            limit=round(ask+0.01,2); reserve=limit*100+0.65
            nav=float(self.portfolio.total_portfolio_value)
            cash=float(self.portfolio.cash_book["USD"].amount)
            if reserve>nav*0.02 or reserve>cash: self._blocked_day=self.time.date(); self._pending=None; return
            security=self.securities[symbol]
            if int(security.symbol_properties.contract_multiplier)!=100: self._fail("MULTIPLIER_INVALID"); return
            security.set_fee_model(PerContractFee()); security.set_fill_model(AdverseLimitFill())
            self._ticket=self.limit_order(symbol,1,limit,tag="T2542I_ENTRY")
        else:
            limit=max(0.01,round(bid-0.01,2)); self._ticket=self.limit_order(symbol,-1,limit,tag="T2542I_EXIT")
        self._submissions+=1; self._pending=None
        if self._submissions>1202: self._fail("ORDER_MAXIMUM_EXCEEDED")

    def _cancel_stale(self):
        if self._ticket is None: return
        order=self.transactions.get_order_by_id(self._ticket.order_id)
        if order is None: return
        if self.time-order.time>=timedelta(minutes=5):
            self._ticket.cancel("T2542I_FIVE_MINUTE_CANCEL"); self._cancels+=1
            self._blocked_day=self.time.date(); self._ticket=None

    def on_data(self,data):
        if self._invalid is not None: return
        day=self.time.date()
        if day not in self._sessions:
            self._sessions.add(day)
            if self._signal(): self._long_sessions+=1
        if float(self.portfolio.cash_book["USD"].amount)<-0.001: self._fail("NEGATIVE_CASH"); return
        if self.portfolio[self._qqq].invested: self._fail("SHARE_DELIVERY_PROHIBITED"); return
        if self._open is not None and abs(float(self.portfolio[self._open].quantity))>1: self._fail("POSITION_MAXIMUM_EXCEEDED"); return
        desired=self._signal(); self._comparator(data,desired); self._cancel_stale()
        self._qqq_quote(data)
        if self._open is not None:
            open_quote=self._quote(data,self._open)
            if open_quote is not None: self._last_open_bid=open_quote[0]; self._last_open_bid_time=self.time
        if self._open is not None and (not desired or self._sessions_to_expiry(self._open)<=7): self._plan_exit()
        self._submit_pending(data)
        if (desired and self._open is None and self._ticket is None and self._pending is None
            and self._selected_day!=day and self._blocked_day!=day
            and (self.time.hour>9 or (self.time.hour==9 and self.time.minute>=31))):
            self._selected_day=day; selected=self._select(data)
            if selected is None: self._no_candidate+=1; self._blocked_day=day
            else: self._pending=("ENTRY",selected[1],self.time)

    def on_order_event(self,event):
        if event.status in (OrderStatus.CANCELED,OrderStatus.INVALID):
            self._blocked_day=self.time.date(); self._ticket=None; return
        if event.status==OrderStatus.PARTIALLY_FILLED: self._fail("PARTIAL_FILL"); return
        if event.status!=OrderStatus.FILLED: return
        self._fills+=1
        order=self.transactions.get_order_by_id(event.order_id)
        if order is None or abs(order.quantity)!=1: self._fail("ORDER_IDENTITY_INVALID"); return
        if order.quantity>0:
            security=self.securities[order.symbol]
            if int(security.symbol_properties.contract_multiplier)!=100: self._fail("MULTIPLIER_INVALID"); return
            self._open=order.symbol; self._entries+=1
        else: self._open=None; self._exits+=1; self._blocked_day=self.time.date()
        self._ticket=None
        if self._fills>1202: self._fail("FILL_MAXIMUM_EXCEEDED")

    def on_assignment_order_event(self,event): self._fail("ASSIGNMENT_OR_EXERCISE")

    def on_end_of_algorithm(self):
        if self._ticket is not None:
            self._ticket.cancel("T2542I_TERMINAL_CANCEL"); self._cancels+=1; self._ticket=None
        valid=self._invalid is None and len(self._sessions)==1202 and self._submissions<=1202 and self._fills<=1202
        if self.portfolio[self._qqq].invested: valid=False; self._invalid=self._invalid or "SHARE_DELIVERY_PROHIBITED"
        terminal=float(self.portfolio.total_portfolio_value)
        if self._open is not None:
            fresh=(self._last_open_bid_time is not None and self._sessions and self._last_open_bid_time.date()==max(self._sessions))
            if self._last_open_bid is None or self._last_open_bid<0 or not fresh: valid=False; self._invalid=self._invalid or "TERMINAL_BID_MISSING_OR_STALE"
            else: terminal-=float(self.portfolio[self._open].holdings_value); terminal+=self._last_open_bid*100
        cmp=self._cmp_factor
        if self._cmp_active:
            fresh=(self._last_qqq_bid_time is not None and self._sessions and self._last_qqq_bid_time.date()==max(self._sessions))
            if self._cmp_entry is None or self._last_qqq_bid is None or not fresh: valid=False; self._invalid=self._invalid or "COMPARATOR_TERMINAL_BID_MISSING_OR_STALE"
            else: cmp*=self._last_qqq_bid/self._cmp_entry
        summary={"schema":"trading_2542i_qc_terminal.v1","status":"PASS" if valid else "INVALID",
          "reason":self._invalid,"requested":"2021-02-22..2025-12-02","evaluated":"2021-02-22..2025-12-02",
          "sessions":len(self._sessions),"expected_sessions":1202,"long_signal_sessions":self._long_sessions,
          "option_return":round(terminal/START_CASH-1,10),"underlying_comparator_return":round(cmp-1,10),
          "order_submissions":self._submissions,"fills":self._fills,"entries":self._entries,"exits":self._exits,
          "no_candidate_sessions":self._no_candidate,"cancels":self._cancels,
          "policy_sha":POLICY_SHA,"package_sha":PACKAGE_SHA,"transition_sha":TRANSITION_SHA,
          "raw_option_rows":False,"contract_identifiers_exported":False,"object_store":False,
          "paper":False,"live":False,"production":False,"broker":"none"}
        self.debug("TRADING2542I_TERMINAL:"+json.dumps(summary,separators=(",",":"),sort_keys=True))
