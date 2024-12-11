# Orders-Data-ETL-Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline for processing orders data. The pipeline reads data from a CSV file, cleans and transforms the data, and saves the output to a new CSV file.

## Table of Contents
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Code Structure](#code-structure)
- [License](#license)

## Overview
The ETL pipeline performs the following steps:
1. **Extract and Clean**: Reads orders data from a CSV file, handles null values, and maps column names.
2. **Transform**: Processes the data, including date conversions, calculation of additional fields, and filtering invalid records.
3. **Load**: Writes the transformed data to a new CSV file.

## Prerequisites
Ensure you have the following installed on your system:
- Python 3.7 or later
- Required Python packages: `pandas`

## Installation
1. Clone the repository or download the script.
   ```bash
   git clone https://github.com/Pedasoft-Consult/Orders-Data-ETL-Pipeline.git
   cd orders-etl
   ```
2. Install the required Python packages using `pip`:
   ```bash
   pip install pandas
   ```

## Usage
1. Place your input dataset (`orders_updated.csv`) in the same directory as the script.
2. Run the script:
   ```bash
   python etl_pipeline.py
   ```
3. The cleaned and transformed data will be saved to `transformed_orders.csv` in the same directory.

### Input
The input file should be a CSV file with appropriate column names such as:
```
SalesOrderID,SalesOrderDetailID,OrderDate,DueDate,ShipDate,EmployeeID,CustomerID,SubTotal,TaxAmt,Freight,TotalDue,ProductID,OrderQty,UnitPrice,LineTotal,UnitPriceDiscount
...
```

### Output
The transformed data is saved into a new CSV file `transformed_orders.csv`. Additional calculated fields include:
- `ProcessingTime`: Days between `OrderDate` and `ShipDate`.
- `IsOverdue`: Boolean indicating if the order was shipped after the due date.
- `UnitPriceDiscountPct`: Discount percentage.
- `OrderCategory`: Categorization based on `TotalDue` values.

## Code Structure
- `etl_pipeline.py`: Contains the ETL logic.

### Functions
#### `extract_and_clean()`
- **Description**: Reads the orders data CSV file, handles null values, and ensures column consistency.
- **Output**: A cleaned Pandas DataFrame.

#### `transform(df)`
- **Description**: Performs transformations such as date conversion, new field calculations, and data filtering.
- **Output**: A transformed Pandas DataFrame.

#### `load(df, output_path, overwrite)`
- **Description**: Saves the transformed data to a specified CSV file. Ensures the output directory exists and checks for overwrite permissions.
- **Parameters**:
  - `output_path`: Path to save the output file.
  - `overwrite`: Boolean to allow overwriting existing files.

## License
This project is licensed under the MIT License. See the LICENSE file for details.
