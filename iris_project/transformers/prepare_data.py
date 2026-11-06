"""
Mage Transformer Block: prepare_data
Splits the raw Iris dataset into training (80%) and testing (20%) partitions.
"""

from sklearn.model_selection import train_test_split
import pandas as pd

# Safe import for Mage decorators: allows block execution both inside Mage and standalone/pytest
if 'transformer' not in globals():
    try:
        from mage_ai.data_preparation.decorators import transformer
    except ImportError:
        def transformer(func):
            return func

if 'test' not in globals():
    try:
        from mage_ai.data_preparation.decorators import test
    except ImportError:
        def test(func):
            return func


@transformer
def transform(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Splits the iris dataset into train and test sets.
    Default split ratio is 80% train (120 rows) and 20% test (30 rows).
    
    Args:
        df: Input pandas DataFrame loaded from sklearn load_iris.
        
    Returns:
        DataFrame with an added 'split' column containing 'train' or 'test'.
    """
    data = df.copy()
    
    # Temporarily changed to 0.3 for deliberate CI red test demonstration
    test_size = kwargs.get('test_size', 0.3)
    
    train_idx, test_idx = train_test_split(
        data.index,
        test_size=test_size,
        random_state=42,
        stratify=data['target'] if 'target' in data.columns else None
    )
    
    data['split'] = 'train'
    data.loc[test_idx, 'split'] = 'test'
    
    return data


@test
def test_output(output, *args) -> None:
    """
    Mage validation test: verifies 80/20 partition proportions.
    """
    assert output is not None, 'The output is undefined'
    assert 'split' in output.columns, "Output dataframe must contain 'split' column"
    assert (output['split'] == 'train').sum() == 120, 'Expected 120 training rows'
    assert (output['split'] == 'test').sum() == 30, 'Expected 30 test rows'
