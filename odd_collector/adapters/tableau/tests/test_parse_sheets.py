from odd_collector.adapters.tableau.domain.sheet import Sheet


def test_sheet_from_response():
    response = {
        "id": "ff156326-811f-fd5e-0a36-c8c02f69a4ad",
        "name": "S&P Forward Returns",
        "createdAt": "2022-08-09T08:12:45Z",
        "updatedAt": "2022-08-09T08:12:45Z",
        "upstreamFields": [
            {
                "id": "40fb92c0-6fe9-9590-8ed2-c290933cc949",
                "name": "Decade",
                "upstreamTables": [{"id": "f275901d-3d2c-aa1d-3ce6-266877fea80d"}],
            },
            {
                "id": "50da20ee-33a4-d048-a4fd-dd144855dafe",
                "name": "Value",
                "upstreamTables": [{"id": "f275901d-3d2c-aa1d-3ce6-266877fea80d"}],
            },
            {
                "id": "ab8d4f55-8215-d607-7087-8b8184366a72",
                "name": "Date",
                "upstreamTables": [{"id": "f275901d-3d2c-aa1d-3ce6-266877fea80d"}],
            },
            {
                "id": "ec4f6949-94fd-3c8f-6f80-b6e0940df045",
                "name": "Metric",
                "upstreamTables": [{"id": "f275901d-3d2c-aa1d-3ce6-266877fea80d"}],
            },
        ],
        "workbook": {"name": "Regional", "owner": {"name": "pmakarichev"}},
    }

    sheet = Sheet.from_response(response)

    assert sheet.name == "S&P Forward Returns"
    assert sheet.id == "ff156326-811f-fd5e-0a36-c8c02f69a4ad"
    assert len(sheet.tables_id) == 1
    assert sheet.tables_id[0] == "f275901d-3d2c-aa1d-3ce6-266877fea80d"
    assert sheet.workbook == "Regional"
    assert sheet.owner == "pmakarichev"
    assert sheet.created == "2022-08-09T08:12:45Z"
    assert sheet.updated == "2022-08-09T08:12:45Z"
