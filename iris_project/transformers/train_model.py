import pandas as pd

from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def train_model(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Train a Decision Tree classifier on the Iris dataset.
    """

    # Get max_depth from runtime variables (default = 3)
    max_depth = int(kwargs.get('max_depth', 3))

    # Separate training and testing data
    train_df = df[df['split'] == 'train']
    test_df = df[df['split'] == 'test']

    # Select feature columns
    feature_columns = [
        'sepal length (cm)',
        'sepal width (cm)',
        'petal length (cm)',
        'petal width (cm)'
    ]

    X_train = train_df[feature_columns]
    y_train = train_df['target']

    X_test = test_df[feature_columns]
    y_test = test_df['target']

    # Train Decision Tree model
    model = DecisionTreeClassifier(
        max_depth=max_depth,
        random_state=42
    )

    model.fit(X_train, y_train)

    # Predict test data
    predictions = model.predict(X_test)

    accuracy = accuracy_score(y_test, predictions)

    print(f"Max depth: {max_depth}")
    print(f"Test accuracy: {accuracy:.4f}")

    # Prepare prediction results
    results = pd.DataFrame({
        'actual': y_test.values,
        'predicted': predictions,
        'max_depth': max_depth,
        'test_accuracy': accuracy
    })

    return results


@test
def test_output(output, *args) -> None:
    """
    Check whether model accuracy meets the minimum requirement.
    """

    assert output is not None, 'Model output is undefined'

    assert 'test_accuracy' in output.columns

    accuracy = float(output['test_accuracy'].iloc[0])

    assert accuracy >= 0.90, (
        f'test_accuracy failed: {accuracy:.4f} is below 0.90'
    )