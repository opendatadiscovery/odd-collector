import os
sheet_name = os.environ['SHEET_NAME'].strip()

SHEET_QUERY = """
{
    sheets(filter: {name: "%s"}) {
        id 
        name
        luid 
            
        path 
        createdAt 
        updatedAt 
        index 

        workbook {
            id 
            name 
            luid 
            site {
                id
                name
            } 
            projectVizportalUrlId 
            projectName 
            owner {
                id
                name
            } 
            uri 
            vizportalUrlId 
            createdAt 
            updatedAt 
        }

        datasourceFields {
            upstreamTables {
                id
                name
                schema 
                database {
                    luid
                }
            }
        }
    }
}
""".replace("%s", sheet_name)
