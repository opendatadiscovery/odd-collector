SHEET_QUERY = """
{
    sheets {
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
"""
