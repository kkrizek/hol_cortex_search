/***************************
 Change database name and warehouse to your database and warehouse
***************************/
USE DATABASE cortex_search_hol;  --CHANGE THIS
USE SCHEMA hol;
USE WAREHOUSE compute_wh;  --CHANGE THIS

CREATE or replace STAGE rag
  DIRECTORY = (enable = true)
  ENCRYPTION = (type = 'snowflake_sse');

LS @rag;

/************************************
 Create a stream to listen for new documents
************************************/
create or replace stream cortex_search_stream on stage rag;

select * from cortex_search_stream;
  
/************************************
 Upload first 4 documents from GIT repository
 https://github.com/kkrizek/hol_cortex_search/tree/main/pdf
************************************/
  
LS @rag;

select * from cortex_search_stream;

/*********************************************
                 PARSE_DOC + SPLIT_TEXT   

The SPLIT_TEXT_RECURSIVE_CHARACTER function splits a string up into smaller chunks of text recursively for 
preprocessing text as input to ML workloads. The function returns an array of text chunks, where the chunks are computed based on the input parameters provided.

Returns the extracted content from a document on a Snowflake stage as an OBJECT that contains JSON-encoded objects as strings. This function supports 2 types of extractions, Optical Character Recognition (OCR) and layout. 

If LAYOUT mode is selected, the data is markdown with structural content including tables.
If OCR, the data is text content
*********************************************/
CREATE TABLE if not exists CHUNKS_PARSE_DOC (
   relative_path VARCHAR
    , scoped_file_url VARCHAR
    , page_content VARIANT
    );

alter table chunks_parse_doc set change_tracking = True;

/***************************************
Create a task to 'listen' for documents to land on the stage.  When a new document lands parse the document, extracting the text and storing it in a variant column.

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
        , GET_PRESIGNED_URL(@cortex_search_hol.hol.rag, relative_path) as scoped_file_url   --CHANGE THIS
        , SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
            to_variant(snowflake.cortex.parse_document(
                @cortex_search_hol.hol.rag,                     --CHANGE THIS
                relative_path,
                {'mode': 'layout'}
            )):content, 'MARKDOWN', 1800, 250) as page_content
    from cortex_search_stream
    where METADATA$ACTION = 'INSERT'
    );

alter task cortex_search_new_docs resume;

select * from chunks_parse_doc;
select * from cortex_search_stream;

/******************************
 Create a dynamic table that listens for new rows on the chunks table and parses each chunk into a separate row.
******************************/

create or replace dynamic table cortex_search_chunks
target_lag = '1 minute'
warehouse = COMPUTE_WH as
  select RELATIVE_PATH, scoped_file_url, 
         'English' as language,
         c.value AS chunk
    from CORTEX_SEARCH_HOL.HOL.CHUNKS_PARSE_DOC D, LATERAL FLATTEN(INPUT => D.page_content) c;   -- CHANGE THIS

select * from cortex_search_hol.hol.CORTEX_SEARCH_CHUNKS;

/*
All of the operational complexity of building the search service is abstracted into a single SQL statement for service creation. This removes the burden of creating and managing multiple processes for ingestion, embedding and serving, ultimately freeing up time to focus on developing cutting-edge AI applications.
*/

CREATE OR REPLACE CORTEX SEARCH SERVICE cortex_search_parse_doc_svc
ON chunk
ATTRIBUTES language
WAREHOUSE = compute_wh
TARGET_LAG = '1 minute'
AS (
SELECT
    chunk::varchar as chunk,
    relative_path,
    language
FROM
    CORTEX_SEARCH_CHUNKS
);

alter task cortex_search_new_docs suspend;
