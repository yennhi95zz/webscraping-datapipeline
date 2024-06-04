import os
import dotenv
import time
from datetime import datetime
from bs4 import BeautifulSoup
from langchain_openai import ChatOpenAI
from langchain_community.document_loaders import AsyncChromiumLoader
from langchain_community.document_transformers import BeautifulSoupTransformer
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.chains import create_extraction_chain
import comet_llm
import nest_asyncio
import warnings

warnings.filterwarnings("ignore")

# Resolve async issues by applying nest_asyncio
nest_asyncio.apply()

# Load environment variables from a .env file
dotenv.load_dotenv()

# Retrieve OpenAI and Comet key from environment variables
MY_OPENAI_KEY = os.getenv("MY_OPENAI_KEY")
MY_COMET_KEY = os.getenv("MY_COMET_KEY")

# Initialize a Comet project
comet_llm.init(project="streaming-webscraping", api_key=MY_COMET_KEY)

# Define a JSON schema for crypto data validation
schema = {
    "properties": {
        "crypto_abbreviation": {"type": "string"},
        "crypto_currency": {"type": "string"},
        "exchange_rate": {"type": "string"},
    },
    "required": ["crypto_abbreviation", "crypto_currency", "exchange_rate"],
}

def generate_crypto_data():
    # Define the URL
    url = "https://www.google.com/finance/markets/cryptocurrencies"

    # Initialize ChatOpenAI instance with OpenAI API key
    llm = ChatOpenAI(openai_api_key=MY_OPENAI_KEY)

    # Load HTML content using AsyncChromiumLoader
    loader = AsyncChromiumLoader([url])
    docs = loader.load()

    # Beautify HTML content
    soup = BeautifulSoup(docs[0].page_content, 'html.parser')

    # Transform the loaded HTML using BeautifulSoupTransformer
    bs_transformer = BeautifulSoupTransformer()
    docs_transformed = bs_transformer.transform_documents(docs, tags_to_extract=["li", "div"])

    # Split the transformed documents using RecursiveCharacterTextSplitter
    splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(chunk_size=1000, chunk_overlap=0)
    splits = splitter.split_documents(docs_transformed)

    # Run the extraction chain with the provided schema and content
    start_time = time.time()
    extracted_content = create_extraction_chain(schema=schema, llm=llm).run(splits[0].page_content)
    extracted_content = [{**item, 'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')} for item in extracted_content]
    for item in extracted_content:
        timestamp = item['timestamp'].replace('-', '').replace(':', '').replace(' ', '')
        integration_key = str(item['crypto_abbreviation']) + '~' + timestamp
        item['integration_key'] = integration_key
        # item['exchange_rate'] = float(item['exchange_rate'].replace(',', ''))

    end_time = time.time()

    # Log metadata and output in the Comet project for tracking purposes
    comet_llm.log_prompt(
        prompt=str(splits[0].page_content),
        metadata={"schema": schema},
        output=extracted_content,
        duration=end_time - start_time,
    )

    return extracted_content
