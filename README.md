# FuelCostHedgeEngine
_Independent Quant-Dev Project — Python, C++/Eigen, pybind11, playwright, Polars,Supabase, cvxpy, NumPy/Numba, Plotly/Streamlit
Oct 2025 – Present_

1. >Built a low-latency telemetry pipeline for Genera PR’s real-time series (generation, capacity, reserves, fuel mix, plant-level MW) using Arrow/Parquet + Polars; achieved sub-100 ms hot-slice queries and 10–20× speedups vs. Pandas baselines.

2. >Engineered a stochastic fuel-cost model with unit heat-rate curves, ramp/start constraints, and reserve requirements; generated scenario paths for load and fuel prices (LNG, bunker, diesel) with shock overlays.

3. >Implemented a co-optimization in cvxpy (LP/QP) to select unit dispatch and fuel hedge notionals that minimize expected cost and CVaR(95) under operational constraints.

4. >Compiled C++ kernels (rolling features, Monte Carlo aggregation) exposed via pybind11; delivered <250 ms end-to-end recompute for 1k-scenario batches on laptop-class hardware.

5. >Validated on stress regimes (low reserves/outage windows): backtests show ≥30% reduction in cost CVaR vs. unhedged baselines and stable reserve margins.

6. >Shipped a what-if dashboard (Plotly/Streamlit) with live clip-to-hedge controls, risk curves, and dispatch/hedge explainability.





> Life isn't about waiting for the storm to pass...It's about learning to dance in the rain. 

_― Vivian Greene_
