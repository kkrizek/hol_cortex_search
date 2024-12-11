/***************************
 Change database name and warehouse to your database and warehouse
***************************/
USE DATABASE cortex_search_docs;  --CHANGE THIS
USE SCHEMA data;
USE WAREHOUSE compute_wh;  --CHANGE THIS

CREATE or replace STAGE docs
  DIRECTORY = (enable = true)
  ENCRYPTION = (type = 'snowflake_sse');

LS @docs;

/************************************
 Create a stream to listen for new documents
************************************/
create or replace stream cortex_search_stream on stage docs;

select * from cortex_search_stream;
  
/************************************
 Upload first 4 documents from GIT repository
 https://github.com/kkrizek/hol_cortex_search/tree/main/pdf
************************************/
  
LS @docs;

select * from cortex_search_stream;

/*********************************************
 Create table to hold the extracted document data
*********************************************/
CREATE TABLE if not exists CHUNKS_PARSE_DOC (
   relative_path VARCHAR
   , size NUMBER
   , file_url VARCHAR
    , scoped_file_url VARCHAR
    , page_content VARIANT
    , category VARCHAR
    );

alter table chunks_parse_doc set change_tracking = True;

/***************************************
                PARSE_DOC + SPLIT_TEXT   

The SPLIT_TEXT_RECURSIVE_CHARACTER function splits a string up into smaller chunks of text recursively for 
preprocessing text as input to ML workloads. The function returns an array of text chunks, where the chunks are computed based on the input parameters provided.

Returns the extracted content from a document on a Snowflake stage as an OBJECT that contains JSON-encoded objects as strings. This function supports 2 types of extractions, Optical Character Recognition (OCR) and layout. 

If LAYOUT mode is selected, the data is markdown with structural content including tables.
If OCR, the data is text content
***************************************/
select
    relative_path
    , size
    , file_url
    , GET_PRESIGNED_URL(@cortex_search_docs.data.docs, relative_path) as scoped_file_url   --CHANGE THIS
    , SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
        to_variant(snowflake.cortex.parse_document(
            @cortex_search_docs.data.docs,                     --CHANGE THIS
            relative_path,
            {'mode': 'layout'}
        )):content, 'MARKDOWN', 1800, 250) as page_content
  from cortex_search_stream
 where METADATA$ACTION = 'INSERT';

/***************************************
We are going to use the power of Large Language Models to easily classify the documents we are ingesting in our RAG application. We are just going to use the file name but you could also use some of the content of the doc itself. Depending on your use case you may want to use different approaches. 

We will pass the file name to the LLM using the Cortex Complete function with a prompt to classify what the guide refers to.  The prompt will be as simple as this but you can try to customize it depending on your use case and documents. Classification is not mandatory for Cortex Search but we want to use it here to also demo hybrid search.
***************************************/
select
    relative_path
    , TRIM(snowflake.cortex.COMPLETE (
            'llama3-70b',
            'Given the name of the file between <file> and </file> determine if it is related to bikes or snow. Use only one word <file> ' ||           
            relative_path || '</file>'), '\n') AS category
    from cortex_search_stream
    where METADATA$ACTION = 'INSERT';

/***************************************
Let's operationalize the previous two statements by putting them in a task that listens for documents to land on the stage.  When a new document lands, we will extract and chunk the contents as well as assign a category.  The extracted text is stored in a variant column.

Change the warehouse name to your warehouse
Change the database to your database
***************************************/
create or replace task cortex_search_new_docs
warehouse = compute_wh                          --CHANGE THIS
schedule = '1 minute'
comment = 'Process new files in the stage and insert data into the chunks_parse_doc table'
when SYSTEM$STREAM_HAS_DATA('cortex_search_stream') as
insert into CHUNKS_PARSE_DOC (
    select
        relative_path
        , size
        , file_url
        , GET_PRESIGNED_URL(@cortex_search_docs.data.docs, relative_path) as scoped_file_url   --CHANGE THIS
        , SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
            to_variant(snowflake.cortex.parse_document(
                @cortex_search_docs.data.docs,                     --CHANGE THIS
                relative_path,
                {'mode': 'layout'}
            )):content, 'MARKDOWN', 1800, 250) as page_content
        , TRIM(snowflake.cortex.COMPLETE (
            'llama3-70b',
            'Given the name of the file between <file> and </file> determine if it is related to bikes or snow. Use only one word <file> ' || 
             relative_path || '</file>'), '\n') AS category
    from cortex_search_stream
    where METADATA$ACTION = 'INSERT'
    );


alter task cortex_search_new_docs resume;

select * from chunks_parse_doc;
select * from cortex_search_stream;

/******************************
The next step is the parse and flatten the extracted text.
******************************/
select RELATIVE_PATH, scoped_file_url, size, file_url,
       c.value AS chunk,
       category
  from CORTEX_SEARCH_DOCS.DATA.CHUNKS_PARSE_DOC D, LATERAL FLATTEN(INPUT => D.page_content) c;   -- CHANGE THIS

/******************************
We can operationalize it by using a dynamic table when new documents are loaded into the CHUNKS_PARSE_DOC table.
******************************/
create or replace dynamic table cortex_search_chunks
target_lag = '1 minute'
warehouse = COMPUTE_WH as
  select RELATIVE_PATH, scoped_file_url, size, file_url,
         c.value AS chunk,
         category
    from CORTEX_SEARCH_DOCS.DATA.CHUNKS_PARSE_DOC D, LATERAL FLATTEN(INPUT => D.page_content) c;   -- CHANGE THIS

select * from cortex_search_docs.data.CORTEX_SEARCH_CHUNKS;

/*
All of the operational complexity of building the search service is abstracted into a single SQL statement for service creation. This removes the burden of creating and managing multiple processes for ingestion, embedding and serving, ultimately freeing up time to focus on developing cutting-edge AI applications.
*/

CREATE OR REPLACE CORTEX SEARCH SERVICE cortex_search_svc
ON chunk
ATTRIBUTES CATEGORY
WAREHOUSE = compute_wh
TARGET_LAG = '1 minute'
AS (
SELECT
    chunk::varchar as chunk,
    relative_path,
    file_url,
    category
FROM
    CORTEX_SEARCH_CHUNKS
);

alter task cortex_search_new_docs suspend;
