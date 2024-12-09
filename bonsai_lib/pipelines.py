import os
import logging
from pathlib import Path

from haystack import Pipeline
from haystack.utils import Secret
from haystack.dataclasses import ChatMessage
from haystack.components.writers import DocumentWriter
from haystack.document_stores.types import DuplicatePolicy
from haystack_integrations.document_stores.chroma import ChromaDocumentStore

from haystack_integrations.components.generators.ollama import OllamaChatGenerator, OllamaGenerator
from haystack.components.generators import HuggingFaceAPIGenerator
from haystack_integrations.components.retrievers.chroma import ChromaEmbeddingRetriever
from haystack.components.builders.prompt_builder import PromptBuilder
from haystack.components.builders.chat_prompt_builder import ChatPromptBuilder
from haystack.components.embedders import SentenceTransformersDocumentEmbedder, SentenceTransformersTextEmbedder

from haystack.components.readers import ExtractiveReader

from haystack.components.converters import PyPDFToDocument, TextFileToDocument
from haystack.components.routers import FileTypeRouter
from haystack.components.joiners import DocumentJoiner
from haystack.components.preprocessors import DocumentSplitter, DocumentCleaner
from haystack.components.fetchers import LinkContentFetcher

from bonsai_lib.components import SubitoItParser, SQLQueryParser, SQLValidator, SQLQuery
from bonsai_lib.prompts import sql_prompt
from bonsai_lib.schema import GeneratorConfig

from haystack_integrations.components.generators.mistral import MistralChatGenerator


class PreprocessingPipeline(Pipeline):
    """
    This class creates a Pipeline for preprocessing the documents.
    It takes a DocumentStore as input and outputs a processed Document.
    The pipeline is composed of the following components:
        - FileTypeRouter: routes the documents to the corresponding converter
        - PyPDFToDocument: converts PDFs to text
        - TextFileToDocument: converts text files to text
        - DocumentJoiner: joins the text of all documents into a single document
        - DocumentCleaner: removes unwanted characters from the text
        - DocumentSplitter: splits the text into sentences
        - SentenceTransformersDocumentEmbedder: embeds the sentences into vectors
        - DocumentWriter: writes the processed documents to the DocumentStore

    """

    def __init__(self, document_store, **kwargs):
        super().__init__()
        """
        
        """
        pdf_converter = PyPDFToDocument()
        text_file_converter = TextFileToDocument()
        document_cleaner = DocumentCleaner()
        document_splitter = DocumentSplitter(split_by="sentence", split_length=1, split_overlap=0)
        document_joiner = DocumentJoiner()
        document_writer = DocumentWriter(document_store=document_store, policy=DuplicatePolicy.OVERWRITE)

        file_type_router = FileTypeRouter(mime_types=["text/plain", "application/pdf", "text/markdown"])
        self.add_component("file_type_router", file_type_router)
        self.add_component("text_file_converter", text_file_converter)
        self.add_component("pypdf_converter", pdf_converter)
        self.add_component("document_joiner", document_joiner)
        self.add_component("document_cleaner", document_cleaner)
        self.add_component("document_splitter", document_splitter)
        self.add_component("embedder", SentenceTransformersDocumentEmbedder(model=kwargs.get("embedder_model")))
        self.add_component("document_writer", document_writer)


        self.connect("file_type_router.text/plain", "text_file_converter.sources")
        self.connect("file_type_router.application/pdf", "pypdf_converter.sources")
        self.connect("text_file_converter", "document_joiner")
        self.connect("pypdf_converter", "document_joiner")
        self.connect("document_joiner", "document_cleaner")
        self.connect("document_cleaner", "document_splitter")
        self.connect("document_splitter", "embedder")
        self.connect("embedder", "document_writer")



class RagPipeline(Pipeline):


    def __init__(self, retriever, ollama_config, prompt, **kwargs):
        
        super().__init__()

        llm = OllamaGenerator(
            url=ollama_config.get("url"), model=ollama_config.get("model"), 
            generation_kwargs=ollama_config.get("generation_kwargs"), 
            timeout=ollama_config.get("timeout")
        )

        # Querying pipeline
        self.add_component("embedder", SentenceTransformersTextEmbedder(model=kwargs.get("embedder_model")))
        self.add_component("retriever", retriever)
        self.add_component("llm", llm)
        self.add_component("prompt_builder", PromptBuilder(prompt))

        self.connect("embedder.embedding", "retriever.query_embedding")
        self.connect("retriever.documents", "prompt_builder.documents")
        self.connect("prompt_builder", "llm")


class SubitoScraperPipeline(Pipeline):
        

        def __init__(self, document_store, **kwargs):
            super().__init__()
            """
            
            """

            fetcher = LinkContentFetcher(user_agents=["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.142.86 Safari/537.36"])
            converter = SubitoItParser()

            self.add_component("fetcher", fetcher)
            self.add_component("converter", converter)
            self.add_component("document_store", document_store)

            self.connect("fetcher.streams", "converter.sources")
            self.connect("converter.documents", "document_store.documents")


class SubitoSearchPipeline(Pipeline):


    def __init__(self, generator_config: GeneratorConfig,  dbname: str, **kwargs):
        
        super().__init__() 

        match generator_config.service:
            case "hugging-face":
                sqlcoder = HuggingFaceAPIGenerator(
                    api_type="serverless_inference_api", 
                    api_params={"model": generator_config.model}, 
                    generation_kwargs=generator_config.generation_kwargs,
                    token=Secret.from_token(generator_config.token)
                )
            case "ollama":
                sqlcoder = OllamaChatGenerator(
                    url=generator_config.url, 
                    model=generator_config.model, 
                    generation_kwargs=generator_config.generation_kwargs, 
                    timeout=generator_config.timeout
                )
                prompt_builder = ChatPromptBuilder(
                    template=[ChatMessage.from_system(sql_prompt)]
                )
            case "mistral":
                sqlcoder = MistralChatGenerator(
                    api_key=Secret.from_token(generator_config.token),
                    model=generator_config.model,
                    generation_kwargs=generator_config.generation_kwargs
                )
                prompt_builder = ChatPromptBuilder(
                    template=[ChatMessage.from_system(sql_prompt)]
                )
            case _:
                raise ValueError(f"Unsupported service: {generator_config.service}")

        sql_query_parser = SQLQueryParser()
        sql_validator = SQLValidator()
        sql_query = SQLQuery(dbname=dbname)

        self.add_component("prompt_builder", prompt_builder)
        self.add_component("sqlcoder", sqlcoder)
        self.add_component("sql_query_parser", sql_query_parser)
        # self.add_component("sql_validator", sql_validator)
        self.add_component("sql_query", sql_query)

        self.connect("prompt_builder.prompt", "sqlcoder")
        self.connect("sqlcoder.replies", "sql_query_parser.replies")
        self.connect("sql_query_parser.replies", "sql_query.queries")

