from openai import OpenAI
import os
import re
from dotenv import load_dotenv
import time

load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
os.environ["OPENAI_API_KEY"] = api_key
client = OpenAI()

def get_files(docs_path, ticker):
  directory_path = os.path.join(docs_path, ticker)
  files = [os.path.join(directory_path, name) for name in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, name))]
  file_names = [name for name in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, name))]
  years = [re.search(r'\b(19|20)\d{2}\b', s).group() for s in file_names]
  return files, years

def run_assistant(file_path,prompt,assistantID):
  assistant = client.beta.assistants.retrieve(assistantID)
  message_file = client.files.create(
    file=open(file_path, "rb"), purpose="assistants"
  )
  user_message = prompt
  thread = client.beta.threads.create(
    messages=[
      {
        "role": "user",
        "content": user_message,
        "attachments": [
          { "file_id": message_file.id, "tools": [{"type": "file_search"}] }
        ],
      }
    ]
  )
  run = client.beta.threads.runs.create_and_poll(
      thread_id=thread.id, assistant_id=assistant.id
  )
  messages = list(client.beta.threads.messages.list(thread_id=thread.id, run_id=run.id))
  message_content = messages[0].content[0].text
  annotations = message_content.annotations
  for index, annotation in enumerate(annotations):
      message_content.value = message_content.value.replace(annotation.text, "")
  return message_content.value

def run_model_over_df(df,sleeptime,prompt,system):
  for i in range(len(df)):
    content = prompt + df.at[i, 'content']
    completion = client.chat.completions.create(
      model="gpt-4o-mini",
      messages=[
        {"role": "system", "content": system},
        {"role": "user", "content": content}
      ]
    )
    df.at[i, 'content'] = completion.choices[0].message.content
    if i % 100 == 0:
      time.sleep(sleeptime)
  return df

def delete_files():
  response=client.files.list()
  for i in range(len(response.data)):
    client.files.delete(response.data[i].id)

def delete_vector_stores():
  response=client.beta.vector_stores.list()
  for i in range(len(response.data)):
    client.beta.vector_stores.delete(response.data[i].id)

def clear_all():
  try:
    delete_files()
  except:
    print('delete files failed')
  try:
    delete_vector_stores()
  except:
    print('delete vector stores failed')

def ask_gpt(query, system_prompt, model="gpt-5-nano"):
  response = client.responses.create(
    model=model,
    input=[
      {
        "role": "system",
        "content": [
          {
            "type": "input_text",
            "text": system_prompt,
          }
        ]
      },
      {
        "role": "user",
        "content": [
          {
            "type": "input_text",
            "text": query
          }
        ]
      }
    ],
  )
  result = response.output[1].content[0].text
  return result


def ask_gpt_formatted(query, system_prompt, output_format, model="gpt-5-nano"):
  response = client.responses.parse(
    model=model,
    input=[
      {
        "role": "system",
        "content": [
          {"type": "input_text", "text": system_prompt}
        ]
      },
      {
        "role": "user",
        "content": [
          {"type": "input_text", "text": query}
        ]
      },
    ],
    text_format=output_format,
  )
  return response.output_parsed

