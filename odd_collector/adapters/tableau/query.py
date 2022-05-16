TABLE_QUERY = """
{
databaseTables {
    id
    vizportalId  
    luid 
    name 
    isEmbedded
    schema
    fullName 
    connectionType 
    description 
    contact {
        id 
        name 
    } 
    isCertified 
    certificationNote 
    certifier {
        id 
        name 
    } 

    database {
        id 
        name 
    }

    columns {
        id 
        vizportalId 
        luid 
        name 
        description 
        remoteType 
    }
}
}
"""

SHEET_QUERY = """
{
    sheets{
        id 
        name 
        luid 
            
        path 
        createdAt 
        updatedAt 
        index 

        workbook{
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

        datasourceFields{
            upstreamTables{
                id
                name
                schema 
                database{
                    id
                    name
                }
            }
        }
    }
}
"""
