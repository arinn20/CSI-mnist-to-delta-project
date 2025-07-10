# MNIST  Delta Table ETL Project

##  What it does
- Reads MNIST hand-written digit images (0–9)
- Samples 5 random images per digit
- Saves data in a managed Delta Table (without compression)

##  Setup
```bash
pip install -r requirements.txt
```

## How to run
```bash
python etl_script.py
```

By default, it uses local folder: `flat files/mnist_png/training/`

##  Check Delta Table
```bash
python sample_output/check_table.py
```

 Put your MNIST image folders under `flat files/mnist_png/training/0`, ..., `9`