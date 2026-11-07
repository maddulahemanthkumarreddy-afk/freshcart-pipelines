import pandas as pd

from sklearn.metrics import confusion_matrix, classification_report

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def evaluate_model(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """Evaluate Iris classifier predictions."""

    actual = df['actual']
    predicted = df['predicted']

    cm = confusion_matrix(actual, predicted, labels=[0, 1, 2])

    print("\nCONFUSION MATRIX")
    print("Rows = Actual; Columns = Predicted")
    print(cm)

    report = classification_report(
        actual,
        predicted,
        labels=[0, 1, 2],
        target_names=['setosa', 'versicolor', 'virginica'],
        output_dict=True,
        zero_division=0
    )

    metrics = pd.DataFrame(report).transpose()

    print("\nCLASSIFICATION METRICS")
    print(metrics.round(4).to_string())

    return metrics.reset_index(names='class')


@test
def test_output(output, *args) -> None:
    assert output is not None
    assert 'class' in output.columns
    assert 'precision' in output.columns
    assert 'recall' in output.columns
    assert 'f1-score' in output.columns