from sklearn.datasets import load_iris
from pandas import DataFrame

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_iris_data(**kwargs) -> DataFrame:
    """
    Load the Iris dataset from scikit-learn.
    """
    iris = load_iris(as_frame=True)

    df = iris.frame.copy()

    # Add readable flower names
    df['target_name'] = df['target'].map(
        dict(enumerate(iris.target_names))
    )

    return df


@test
def test_output(df, *args) -> None:
    """
    Validate the loaded Iris dataset.
    """
    assert df is not None, 'Dataset is empty'
    assert len(df) == 150, 'Expected 150 Iris records'
    assert 'target' in df.columns, 'Target column missing'
    assert df['target'].nunique() == 3, 'Expected 3 classes'
    assert df.isnull().sum().sum() == 0, 'Missing values found'