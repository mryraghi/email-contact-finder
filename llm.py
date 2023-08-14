import os
from langchain.llms import OpenAI
from langchain.chat_models import ChatOpenAI
from dotenv import load_dotenv
from langchain import PromptTemplate
from langchain.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field, validator
from typing import List

# Load variables from .env
load_dotenv()

# Create an instance of the OpenAI class
model = OpenAI(
    openai_api_key=os.getenv("OPENAI_API_KEY"),
    model_name="gpt-4",
    temperature=0,
)


class ContactInfo(BaseModel):
    contact_us_urls: List[str] = Field(description="list of urls of contact us pages")
    # emailAddress: str = Field(description="email address of the business")

    # You can add custom validation logic easily with Pydantic.
    # @validator("setup")
    # def question_ends_with_question_mark(cls, field):
    #     if field[-1] != "?":
    #         raise ValueError("Badly formed question!")
    #     return field


def get_contact_url_from_list_of_urls(urls):
    # Set up a parser + inject instructions into the prompt template.
    parser = PydanticOutputParser(pydantic_object=ContactInfo)

    prompt = PromptTemplate(
        template="You are an AI tasked to extract the most likely contact us pages from the following list of URL.\n"
        + "{format_instructions}\n\nThe list:\n- {urls}",
        input_variables=["urls"],
        partial_variables={"format_instructions": parser.get_format_instructions()},
    )

    _input = prompt.format_prompt(urls=urls)

    print(_input.to_string())
    output = model(_input.to_string())
    print(output)
    return parser.parse(output)
