# Pre-Open Limit Up Strategy Guide (竞价涨停预测)

This task (`run_limit_up_pre_open.py`) is designed to capture strong limit-up signals during the **Call Auction (09:25)** phase.

## ⚠️ Prerequisites

1.  **Operating System**: Windows (Required for XtQuant / MiniQMT).
2.  **Software**: 
    -   **MiniQMT** (Quant Trading Client) must be running.
    -   **Python**: Installed with `xtquant` library.
3.  **Permissions**: Ensure MiniQMT has "Download Historical Data" permissions if backfilling, though this script uses real-time subscription.

## 🚀 Installation

1.  **Clone the Repository** on your Windows trading machine:
    ```powershell
    git clone <your-repo-url>
    cd quant_data_engine
    ```

2.  **Install Dependencies**:
    ```powershell
    pip install -e .
    ```

## 🏃 Usage

### 1. Run Once (Test Mode)
Run immediately to verify connectivity and logic (will only analyze if time is close to 09:25 or if modified for testing).
```powershell
python -m quant_data_engine.tasks.run_limit_up_pre_open --once
```

### 2. Daemon Mode (Recommended)
Run as a background service. It will sleep and automatically wake up at **09:25** every trading day.
```powershell
python -m quant_data_engine.tasks.run_limit_up_pre_open --daemon
```

## 🧠 Strategy Logic

The script triggers exactly at **09:25:00** and filters stocks based on:

1.  **Open Increase**: +2% to +8% (`0.02 <= open_inc <= 0.08`)
2.  **Volume Ratio**: > 3 (`volume_ratio > 3`)
3.  **Bid Ratio (WeiBi)**: > 30% (`bid_ratio > 0.3`)

## 📂 Output

Results are logged to the console and standard log files.
-   **Console**: Prints formatted candidates.
-   **Logs**: `logs/run_limit_up_pre_open.log` (if configured in logging settings).

## 🛠 Troubleshooting

-   **"xtquant not found"**: Ensure you are using the Python environment where `xtquant` is installed (usually bundled with MiniQMT or installed manually).
-   **"Connection failed"**: Check if MiniQMT is running and the port (default 5899) is open.
