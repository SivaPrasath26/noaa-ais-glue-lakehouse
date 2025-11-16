from utils.schema_definitions import SCHEMA_MAP

# Test to verify raw schema columns, types, and DataFrame conformity
def test_raw_schema_columns():
    try:
        schema = SCHEMA_MAP["raw"]

        expected_cols = [f.name for f in schema.fields]

        print("raw schema columns:", expected_cols)

        # basic checks
        assert isinstance(expected_cols, list)
        assert len(expected_cols) > 0

    except Exception as e:
        print("schema error:", e)
        raise

# Test to verify raw schema field types
def test_raw_schema_types():
    try:
        schema = SCHEMA_MAP["raw"]

        for f in schema.fields:
            print(f.name, str(f.dataType))
            assert f.dataType is not None

    except Exception as e:
        print("schema type error:", e)
        raise

# Test to verify DataFrame conforms to raw schema
def test_input_df_matches_schema(spark):
    try:
        schema = SCHEMA_MAP["raw"]
        expected_cols = [f.name for f in schema.fields]

        # create df with all expected columns but empty data
        df = spark.createDataFrame([], schema)

        print("df columns:", df.columns)

        # check columns match exactly
        assert df.columns == expected_cols

    except Exception as e:
        print("schema match error:", e)
        raise

# Test to verify schema violations are detected
def test_schema_no_extra_fields(spark):
    try:
        schema = SCHEMA_MAP["raw"]
        expected_cols = set([f.name for f in schema.fields])

        # create df with an intentional extra field
        df = spark.createDataFrame(
            [(1, 2, 3)],
            ["MMSI", "LAT", "EXTRA_COL"]
        )

        extra = set(df.columns) - expected_cols
        print("extra columns:", extra)

        assert "EXTRA_COL" in extra

    except Exception as e:
        print("extra field detection error:", e)
        raise

# Test to verify missing schema fields are detected
def test_schema_missing_fields(spark):
    try:
        schema = SCHEMA_MAP["raw"]
        expected_cols = set([f.name for f in schema.fields])

        # create df missing an expected field
        df = spark.createDataFrame(
            [(1, 2)],
            ["MMSI", "LAT"]
        )

        missing = expected_cols - set(df.columns)
        print("missing columns:", missing)

        assert "LON" in missing

    except Exception as e:
        print("missing field detection error:", e)
        raise

# Test to verify schema field types are enforced
def test_schema_field_types(spark):
    try:
        schema = SCHEMA_MAP["raw"]

        # create df with correct columns but wrong types
        df = spark.createDataFrame(
            [("not_an_int", "not_a_float")],
            ["MMSI", "LAT"]
        )

        for f in schema.fields:
            col_type = dict(df.dtypes).get(f.name)
            expected_type = str(f.dataType).lower()
            print(f"Column: {f.name}, DataFrame type: {col_type}, Expected type: {expected_type}")
            assert expected_type in col_type

    except Exception as e:
        print("field type mismatch detection error:", e)
        raise

# Test to verify non-nullable schema fields are enforced
def test_schema_nullable_fields(spark):
    try:
        schema = SCHEMA_MAP["raw"]

        # create df with nulls in non-nullable fields
        df = spark.createDataFrame(
            [(None, 45.0)],
            ["MMSI", "LAT"]
        )

        non_nullable_fields = [f.name for f in schema.fields if not f.nullable]
        print("Non-nullable fields:", non_nullable_fields)

        for field in non_nullable_fields:
            null_count = df.filter(df[field].isNull()).count()
            print(f"Field: {field}, Null count: {null_count}")
            assert null_count == 0

    except Exception as e:
        print("nullable field violation detection error:", e)
        raise