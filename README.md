# Build a Chat with your Documents App using RAG with Snowflake Cortex

This hands-on lab is intended to be delivered in approximately 60 minutes.  It walks attendees through creating a document processing pipeline, creating a cortex search service, and build two Streamlit apps to provide a natural language interface to chat with your documents.  It is based upon the quickstart linked below:

https://quickstarts.snowflake.com/guide/ask_questions_to_your_own_documents_with_snowflake_cortex_search/index.html#0

Hands-on Lab Flow:
- Introduction and provide attendees with account access information
- Google Slide Presentation (5 minutes)
- Review the Premium Bicycle User Guide calling out the highlighted text
- Create a new notebook from the notebook in the Git repository
  - The notebook creates a document pipeline with a stream, task, dynamic table, and a search service
  - The notebook also contains the source code for two Streamlit apps
     - Streamlit App1:  no chat history
     - Streamlit App2:  chat history
