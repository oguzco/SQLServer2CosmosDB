const CosmosClient = require('@azure/cosmos').CosmosClient;
const sql = require("mssql");

let _log = console.log;
console.log = (...args) => {
    _log("LOG",new Date().toISOString(), ...args);
};
console.error = (...args) =>{
    _log("ERR",new Date().toISOString(),...args);
};
console.warn = (...args)=>{
    _log("WRN",new Date().toISOString(),...args );
}; 

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
let processNextRow = async()=>{
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


    //Run The Query
    let queryResultsets = await new Promise(resolve => sqlRequest.query(query,(err,res) => resolve(err || res)));

    //Cosmos Stuff
    try{
        //Check Query Response
        if( queryResultsets == null )
            throw "Query response was null somehow";

        //if no recordsets exist in the response, then it was an error...
        if( queryResultsets.recordsets == null )
            throw queryResultsets;

        //Query Ran Successfully but did not output any rows..
        if( queryResultsets.recordsets[0].length === 0 ){
            console.log("No records found in resultset",queryResultsets);
            setTimeout( processNextRow, 600000 );
            return;
        }

        //Get That 1 row to be processed next
        let row = queryResultsets.recordsets[0][0];

        //Send Row To Cosmos
        let result = await new Promise( (resolve,reject) => cosmos.database( process.env.COSMOS_DATABASE ).container( process.env.COSMOS_CONTAINER ).items.upsert(row).then(resolve).catch(reject));

        //Check result if it were actually successful
        if( result == null || result.statusCode != 201 ){
            //Log the strange result
            console.error("Cosmos request failed somehow", result);

            //Retry in 1 minute
            setTimeout( processNextRow, 600000 );
        }
        else{
            //No Exception occured so successfully completed
            if( process.env.SQL_DELETE == "1" )
                await deleteRow( row[ process.env.SQL_TABLE_PK ] );
            else
                currentOffset++;

            //Get To Next Row
            setTimeout( processNextRow,0 );
        }    
    }
    catch(exc){
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
        //Request Size too large
        else if(exc.code == 413){
            //skip this row
            currentOffset++;
            //continue
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
    }

};

sqlPool.connect().catch(console.error).then(processNextRow);