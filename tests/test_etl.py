from etl import load_csv

def test_load_csv(tmp_path):
    p = tmp_path / "x.csv"
    p.write_text("sku,qty,price\nX,1,2.5\n")
    rows = load_csv(str(p))
    assert rows == [("X",1,2.5)]
