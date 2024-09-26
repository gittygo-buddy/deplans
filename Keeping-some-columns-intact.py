from datetime import datetime

import pandas as pd
from sdv.evaluation.single_table import evaluate_quality, run_diagnostic
from sdv.metadata import SingleTableMetadata
from sdv.single_table import CTGANSynthesizer


constrains_column = ['Quantity', 'Total Price']


diagnose = False
export = True
file_name = r'C:\Users\saman\Downloads\stock_missingcol.csv'
current_time = datetime.now().strftime("%Y%m%d%H%M%S")
file_output_name = f'{file_name}_{current_time}_CTGAN_gen_synthetic.csv'

real_data = pd.read_csv(file_name)
print(f"Real data lines: {real_data.count()}")
print(real_data.head(10))

# Create metadata
metadata = SingleTableMetadata()
metadata.detect_from_dataframe(real_data)

synthesizer = CTGANSynthesizer(
    metadata,
    epochs=2,
    verbose=True
)
synthesizer.load_custom_constraint_classes('OriginalFixedCombinations.py', class_names=['OriginalFixed'])
if all(column in real_data.columns for column in constrains_column):
    for column in constrains_column:
        if column in metadata.columns.keys():
            print(f"Column {column} update to categorical.")
            metadata.columns.get(column)['sdtype'] = 'categorical'
my_constraint = {
    'constraint_class': 'OriginalFixed',
    'constraint_parameters': {
        'column_names': constrains_column
    }
}
synthesizer.add_constraints([my_constraint])

synthesizer.fit(real_data)
# sample
synthetic_data = synthesizer.sample(num_rows=1000)
print(synthetic_data.head())
if export:
    synthetic_data.to_csv(file_output_name, index=False)
# evaluation
if diagnose:
    diagnostic = run_diagnostic(
        real_data=real_data,
        synthetic_data=synthetic_data,
        metadata=metadata
    )
    quality_report = evaluate_quality(
        real_data,
        synthetic_data,
        metadata
    )
    print(quality_report.get_details('Column Shapes'))
