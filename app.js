const CosmosClient = require('@azure/cosmos').CosmosClient;
const sql = require("mssql");


let cosmos = new CosmosClient({
    endpoint: process.env.COSMOS_ENDPOINT,
    key: process.env.COSMOS_KEY
});

let sqlPool = new sql.ConnectionPool({
    user: process.env.SQL_USER,
    password: process.env.SQL_PASS,
    server: process.env.SQL_ADDR,
    database: process.env.SQL_DBNAME,
    options: {
        appName: "SQLServer2CosmosDB",
        encrypt: false
    },
    stream: true,
    pool: {
        "min": 3,
        "max": 3,
        "idleTimeoutMillis": 2147483647
    }
});

let deleteRow = async ( pk )=>{
    let sqlRequest = sqlPool.request();
    sqlRequest.input(process.env.SQL_TABLE_PK,pk);
    
    let sqlResult = new Promise((resolve,reject)=>{
        sqlRequest.query(`DELETE TOP(1) ${process.env.SQL_TABLE} WHERE ${process.env.SQL_TABLE_PK}=@${process.env.SQL_TABLE_PK}`,(queryError,queryResults)=>{
            if(queryError){
                console.error(queryError);
                //Delete was requested but we failed, so we should stop here
                if( process.env.SQL_DELETE == "1" )
                    process.exit(-1);
            }
                
            resolve();
        });
    });
    
    await sqlResult;
};

let currentOffset = 0;
let processNextRow = ()=>{
    let query = null;
    if(currentOffset == 0){
        //No paging stuff required
        query = `SELECT TOP(1) * FROM ${process.env.SQL_TABLE}`;
    }else{
        //Rows must be paged
        query = `SELECT * 
        FROM ${process.env.SQL_TABLE} 
        ORDER BY ${process.env.SQL_TABLE_PK} 
        OFFSET @OFFSET ROWS 
        FETCH NEXT 1 ROWS ONLY`;
    }

    let sqlRequest = sqlPool.request();
    sqlRequest.input("OFFSET",currentOffset);
    sqlRequest.query(query, async(queryError,queryResultsets)=>{
        if( queryError ){
            console.error(queryError);
            process.exit(-1);
        }else{
            let row = queryResultsets.recordsets[0][0];

            cosmos
            .database( process.env.COSMOS_DATABASE ).container( process.env.COSMOS_CONTAINER )
            .items
            .upsert(row)
            .then(async () => {
                if( process.env.SQL_DELETE == "1" )
                    await deleteRow( row[ process.env.SQL_TABLE_PK ] );
                else
                    currentOffset++;

                setTimeout( processNextRow,0 );
            })
            .catch(async(exc) => {
                //Conflict
                if(exc.code == 409){
                    //Update or Override or Merge

                    //Continue directly
                    if( process.env.SQL_DELETE == "1" )
                        await deleteRow( row[ process.env.SQL_TABLE_PK ] );
                    else
                        currentOffset++;
                    setTimeout( processNextRow,0 );
                }
                //Wait Required, Too Many Requests
                else if(exc.code == 429) {
                    setTimeout( processNextRow,10000 );
                }
                //Unhandled yet
                else{
                    console.log(exc.code);
                    console.error(exc);
                    process.exit(-1);
                }
            });                
        }
    });
};

sqlPool.connect().catch(console.error).then(processNextRow);