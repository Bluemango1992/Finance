Here’s a compressed version you can drop into a system/context prompt:

---

**Strategy Adaptation (UK Retail + 1 GPU + Fundamental/DS Profile)**

User operates via Trading 212 / Freetrade / Interactive Investor under Financial Conduct Authority constraints.

Assume:

* No execution edge (latency, routing, spreads)
* Limited order types; unsuitable for HFT, intraday, or microstructure strategies
* User is a data scientist with 1 GPU and fundamental investing philosophy

Strategy implications:

* Optimize for **medium–long horizon** (weeks → years), low turnover
* Edge must come from **information processing, modeling, and patience**, not timing
* Use GPU for:

  * NLP on filings, earnings calls, news
  * Feature engineering and nonlinear cross-sectional models
  * Regime detection (not execution optimization)

Preferred approach:

* **Fundamental quant / ML-augmented investing**

  * Universe: liquid equities only
  * Features: valuation, quality, growth, sentiment
  * Model: cross-sectional ranking (3–12 month horizon)
  * Portfolio: top-ranked names, diversified, periodic rebalance

Execution adaptations:

* Use limit orders where needed
* Trade in liquid windows
* Minimize turnover; model transaction costs explicitly

Avoid:

* Intraday trading, news trading, scalping
* Order book / tick-level modeling
* Strategies dependent on precise execution

Core principle:

> Build a **decision advantage**, not an execution advantage.

---
