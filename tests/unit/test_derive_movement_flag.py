from utils.common_functions import derive_movement_flag

# Test deriving MovementFlag based on SOG
def test_derive_movement_flag(spark):
    df = spark.createDataFrame(
        [(0,), (5,)],
        ["SOG"]
    )
    out = derive_movement_flag(df)
    vals = [r.MovementFlag for r in out.collect()]
    assert vals == [0, 1]
