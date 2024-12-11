import pandas as pd
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_and_clean():
    """
    Extracts data from a CSV file into a Pandas DataFrame, cleans null values,
    and maps column names to expected values.
    """
    file_path = './orders_updated.csv'

    if not os.path.exists(file_path):
        logging.error(f"File '{file_path}' not found.")
        return None

    logging.info("Starting to read the CSV file.")
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Data loaded successfully with {len(df)} rows and {len(df.columns)} columns.")

        logging.info("Checking for null values.")
        null_summary = df.isnull().sum()
        logging.info("\n%s", null_summary)

        # Clean null values
        df = df.assign(
            SalesOrderID=df.get('SalesOrderID', pd.Series()).fillna(0),
            SalesOrderDetailID=df.get('SalesOrderDetailID', pd.Series()).fillna(0),
            OrderDate=df.get('OrderDate', pd.Series()).fillna('1900-01-01'),
            DueDate=df.get('DueDate', pd.Series()).fillna('1900-01-01'),
            ShipDate=df.get('ShipDate', pd.Series()).fillna('1900-01-01'),
            EmployeeID=df.get('EmployeeID', pd.Series()).fillna(-1),
            CustomerID=df.get('CustomerID', pd.Series()).fillna(-1),
            SubTotal=df.get('SubTotal', pd.Series()).fillna(0.0),
            TaxAmt=df.get('TaxAmt', pd.Series()).fillna(0.0),
            Freight=df.get('Freight', pd.Series()).fillna(0.0),
            TotalDue=df.get('TotalDue', pd.Series()).fillna(
                df['SubTotal'] + df['TaxAmt'] + df['Freight']
            ),
            ProductID=df.get('ProductID', pd.Series()).fillna(-1),
            OrderQty=df.get('OrderQty', pd.Series()).fillna(1),
            UnitPrice=df.get('UnitPrice', pd.Series()).fillna(0.0),
            LineTotal=df.get('LineTotal', pd.Series()).fillna(
                df['OrderQty'] * df['UnitPrice']
            ),
            UnitPriceDiscount=df.get('UnitPriceDiscount', pd.Series()).fillna(0.0)
        )

        logging.info("Cleaned data preview:")
        logging.info("\n%s", df.head().to_string())

        return df

    except Exception as e:
        logging.error(f"Unexpected error while reading the file: {e}")
        return None

def transform(df):
    logging.info("Starting data transformation.")
    try:
        date_columns = ['OrderDate', 'DueDate', 'ShipDate']
        df[date_columns] = df[date_columns].apply(pd.to_datetime, errors='coerce')

        df['ProcessingTime'] = (df['ShipDate'] - df['OrderDate']).dt.days.fillna(-1)
        df['IsOverdue'] = (df['ShipDate'] > df['DueDate']).fillna(False)
        df['UnitPriceDiscountPct'] = (df['UnitPriceDiscount'] * 100).round(2)

        order_category_bins = [0, 1000, 5000, 10000, float('inf')]
        order_category_labels = ['Low', 'Medium', 'High', 'Very High']
        df['OrderCategory'] = pd.cut(
            df['TotalDue'],
            bins=order_category_bins,
            labels=order_category_labels,
            include_lowest=True
        )

        df = df[df['OrderQty'] > 0]
        df['LineTotal'] = (df['OrderQty'] * df['UnitPrice']).round(2).fillna(0.0)
        essential_columns = ['OrderDate', 'DueDate', 'ShipDate', 'OrderQty', 'TotalDue', 'LineTotal']
        df = df.dropna(subset=essential_columns)

        logging.info(f"Transformation complete. Final row count: {len(df)}.")
        return df

    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        return None

def load(df, output_path='./transformed_orders.csv', overwrite=False):
    logging.info("Starting the load process.")
    try:
        output_dir = os.path.dirname(output_path) or '.'
        os.makedirs(output_dir, exist_ok=True)

        if os.path.exists(output_path) and not overwrite:
            logging.error(f"File '{output_path}' already exists. Set overwrite=True to overwrite it.")
            return False

        df.to_csv(output_path, index=False)
        logging.info(f"Data successfully saved to '{output_path}'.")
        return True

    except Exception as e:
        logging.error(f"Error occurred during the load process: {e}")
        return False

# Execute ETL
cleaned_data = extract_and_clean()
if cleaned_data is not None:
    transformed_data = transform(cleaned_data)
    if transformed_data is not None:
        if load(transformed_data, output_path='./transformed_orders.csv', overwrite=True):
            logging.info("ETL process completed successfully.")
        else:
            logging.error("Data load failed.")
    else:
        logging.error("Transformation failed.")
else:
    logging.error("Extraction and cleaning failed.")
