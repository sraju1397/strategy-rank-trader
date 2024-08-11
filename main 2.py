import os
import chardet
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows


def detect_encoding(file_path):
    with open(file_path, 'rb') as file:
        raw_data = file.read()
    return chardet.detect(raw_data)['encoding']


def read_csv_file(file_path):
    try:
        encoding = detect_encoding(file_path)
        df = pd.read_csv(file_path, encoding=encoding)
        df['Strategy'] = os.path.splitext(os.path.basename(file_path))[0]  # Use filename as strategy name
        return df
    except Exception as e:
        print(f"Error reading file {file_path}: {str(e)}")
        return None


def preprocess_dataframe(df):
    df.columns = df.columns.str.lower()
    df['entry_date'] = pd.to_datetime(df['entry_date'], errors='coerce').dt.strftime('%Y-%m-%d')
    return df.dropna(subset=['entry_date', 'profit'])


def apply_frequency(df, frequency):
    df['entry_date'] = pd.to_datetime(df['entry_date'])
    if frequency.capitalize() == 'Weekly':
        df['period'] = df['entry_date'] - pd.to_timedelta(df['entry_date'].dt.dayofweek, unit='D')
    elif frequency.capitalize() == 'Monthly':
        df['period'] = df['entry_date'].dt.to_period('M').dt.to_timestamp()
    elif frequency.capitalize() == 'Quarterly':
        df['period'] = df['entry_date'].dt.to_period('Q').dt.to_timestamp()
    else:  # Daily
        df['period'] = df['entry_date']
    df['period'] = df['period'].dt.strftime('%Y-%m-%d')
    return df


def group_and_pivot_data(df):
    grouped_df = df.groupby(['period', 'strategy'])['profit'].sum().reset_index()
    pivot_df = grouped_df.pivot(index='period', columns='strategy', values='profit').reset_index()
    return pivot_df.fillna(0).sort_values('period')


def select_best_strategy(pivot_df, rank_filter):
    strategy_columns = pivot_df.columns.drop(['period'])
    selected_strategies = []
    selected_profits = []

    for i in range(len(pivot_df)):
        if i == 0:
            # For the first row, use its own data (no previous row available)
            profits = pivot_df.iloc[0][strategy_columns]
        else:
            # For subsequent rows, use previous row's data for ranking
            profits = pivot_df.iloc[i - 1][strategy_columns]

        profits = profits.apply(pd.to_numeric, errors='coerce')
        ranks = profits.rank(method='min', ascending=False)
        selected_strategy = ranks[ranks == rank_filter].index[0] if rank_filter in ranks.values else ranks.idxmin()

        selected_strategies.append(selected_strategy)
        selected_profits.append(pivot_df.iloc[i][selected_strategy])

    return pd.DataFrame({
        'Date': pivot_df['period'],
        'Strategy': selected_strategies,
        'Profit': selected_profits
    })


def save_to_excel(df, file_name):
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    wb = Workbook()
    ws = wb.active

    for r in dataframe_to_rows(df, index=False, header=True):
        ws.append(r)

    full_path = os.path.join(output_dir, file_name)
    wb.save(full_path)
    print(f"Data saved to {full_path}")


def process_csv_files(folder_path, frequency='Daily', rank_filter=1):
    try:
        all_data = [df for file in os.listdir(folder_path)
                    if file.endswith('.csv')
                    and (df := read_csv_file(os.path.join(folder_path, file))) is not None]

        if not all_data:
            print(f"No valid CSV files found in {folder_path}")
            return pd.DataFrame(), pd.DataFrame()

        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df = preprocess_dataframe(combined_df)
        combined_df = apply_frequency(combined_df, frequency)

        pivot_df = group_and_pivot_data(combined_df)
        final_df = select_best_strategy(pivot_df, rank_filter)

        return final_df, pivot_df

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(), pd.DataFrame()


def main():
    folder_path = os.path.join('data', 'STRATEGIES')
    frequencies = ['Daily', 'Weekly', 'Monthly', 'Quarterly']
    rank_filter = 1  # This can be changed as needed

    for frequency in frequencies:
        best_strategy_df, full_analysis_df = process_csv_files(folder_path, frequency=frequency,
                                                               rank_filter=rank_filter)
        if not best_strategy_df.empty:
            save_to_excel(best_strategy_df, f'best_strategy_{frequency.lower()}.xlsx')
        if not full_analysis_df.empty:
            save_to_excel(full_analysis_df, f'full_analysis_{frequency.lower()}.xlsx')


if __name__ == "__main__":
    main()
