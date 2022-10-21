"""
Función para subir archivos al Azure Blob Storage
Previo a esto se debe instalar la biblioteca azure.storage.blob
(For linux: pip install azure-storage-blob)
"""

import time
import sys
import requests
import os
import json
import swagger_client as python_client
from azure.storage.blob import BlobServiceClient
from utils import transformar_audio_wav


class AzureTranscription:
   def __init__(
        self,        
        subscription_key_storage_account="",
        azure_region="",
        api_stt="api.cognitive.microsoft.com/speechtotext/v3.0",
        properties=dict(),
        name_transcription="Simple transcription",
        description_transcription="Simple transcription description",
        language="es-ES",
        model_reference=None,
        blob_uri="",
        container_uri="",
        transcription_id="",
        destinationContainerUrl="",        
        *args,
        **kwargs,
   ):
      """Propiertes = {
         "punctuationMode": "DictatedAndAutomatic",
         "profanityFilterMode": "Masked",
         "wordLevelTimestampsEnabled": True,
         "diarizationEnabled": True,
         "destinationContainerUrl": "",
         "timeToLive": "PT1H",}
          URI : RECORDINGS_BLOB_URI i RECORDING_CONTAINER_URI
      Args:
          azure_region (str, optional): _description_. Defaults to "".
          api_stt (str, optional): _description_. Defaults to "api.cognitive.microsoft.com/speechtotext/v3.0".
          properties (dict, optional): _description_. Defaults to { "punctuationMode": "DictatedAndAutomatic", "profanityFilterMode": "Masked", "wordLevelTimestampsEnabled": True, "diarizationEnabled": True, "destinationContainerUrl": "", "timeToLive": "PT1H",}.
      """
      self.subscription_key_storage_account = subscription_key_storage_account
      self.azure_region = azure_region
      self.api_stt = api_stt
      self.name_transcription = name_transcription
      self.description_transcription = description_transcription
      self.language = language
      self.model_reference = model_reference
      self.blob_uri = blob_uri
      self.container_uri = container_uri
      self.transcription_id = transcription_id
      # configure API key authorization: subscription_key
      self.configuration = python_client.Configuration()
      self.configuration.api_key[
          "Ocp-Apim-Subscription-Key"
      ] = self.subscription_key_storage_account
      self.configuration.host = f"https://{self.azure_region}.{self.api_stt}"
      # create the client object and authenticate
      self.client = python_client.ApiClient(self.configuration)
      # create an instance of the transcription api class
      self.api = python_client.CustomSpeechTranscriptionsApi(api_client=self.client)
      # Specify transcription properties by passing a dict to the properties parameter. See
      # https://docs.microsoft.com/azure/cognitive-services/speech-service/batch-transcription#configuration-properties
      # for supported parameters.
      if not properties:
         self.properties = {
         "punctuationMode": "DictatedAndAutomatic",
         "profanityFilterMode": "Masked",
         "wordLevelTimestampsEnabled": True,
         "diarizationEnabled": True,
         "destinationContainerUrl": destinationContainerUrl,
         "timeToLive": "PT1H",}
      else:
       self.properties = properties

   def transcribe_from_single_blob(self):
        """
        Transcribe a single audio file located at `uri` using the
        settings specified in `properties`
        using the base model for the specified locale.
        """
        transcription_definition = python_client.Transcription(
            display_name=self.name_transcription,
            description=self.description_transcription,
            locale=self.language,
            content_urls=[self.blob_uri],
            properties=self.properties,
        )
        return transcription_definition

   def transcribe_with_custom_model(self):
      """
      Transcribe a single audio file located at `uri` using 
      the settings specified in `properties`
      using the base model for the specified locale.
      """
      # Model information (ADAPTED_ACOUSTIC_ID and ADAPTED_LANGUAGE_ID) 
      # must be set above.
      if self.model_reference is None:
          print("Custom model ids must be set when using custom models")
          sys.exit()
      model = self.api.get_model(self.model_reference)
      transcription_definition = python_client.Transcription(
          display_name=self.name_transcription,
          description=self.description_transcription,
          locale=self.language,
          content_urls=[self.blob_uri],
          model=model,
          properties=self.properties,
      )
      return transcription_definition
   
   def transcribe_from_container(self):
      """
      Transcribe all files in the container located at `self.container_uri` 
      using the settings specified in `properties`
      using the base model for the specified locale.
      """
      transcription_definition = python_client.Transcription(
          display_name=self.name_transcription,
          description=self.description_transcription,
          locale=self.language,
          content_container_url=self.container_uri,
          properties=self.properties,
      )
      return transcription_definition

   def _paginate(self, paginated_object):
      """
      The autogenerated client does not support pagination. This function
      returns a generator over
      all items of the array that the paginated object `paginated_object` is part of.
      """
      yield from paginated_object.values
      typename = type(paginated_object).__name__
      auth_settings = ["apiKeyHeader", "apiKeyQuery"]
      while paginated_object.next_link:
          link = paginated_object.next_link[len(self.api.api_client.configuration.host) :]
          paginated_object, status, headers = self.api.api_client.call_api(
              link, "GET", response_type=typename, auth_settings=auth_settings
          )
          if status == 200:
              yield from paginated_object.values
          else:
              raise Exception(f"could not receive paginated data: status {status}")

   def delete_all_transcriptions(self):
      """
      Delete all transcriptions associated with your speech resource.
      """
      print("Deleting all existing completed transcriptions.")
      # get all transcriptions for the subscription
      transcriptions = list(self._paginate(self.api.get_transcriptions()))
      # Delete all pre-existing completed transcriptions.
      # If transcriptions are still running or not started, they will not be deleted.
      for transcription in transcriptions:
          transcription_id = transcription._self.split("/")[-1]
          print(f"Deleting transcription with id {transcription_id}")
          try:
              self.api.delete_transcription(transcription_id)
          except python_client.rest.ApiException as exc:
              print(f"Could not delete transcription {transcription_id}: {exc}")
   
   def make_transcription(self,transcription_type):
      """Begin the transcription

      Args:
          transcription_type (_int_): 
            0:'simple_transcription
            1: transcription with custom model
            2: containter transcription
      Returns:
         True: all ok
         False: error
      """
      if transcription_type <0 or transcription_type >3:
         print("transcription_type is not correct")
         return False
      elif transcription_type == 0:
         transcription_definition = self.transcribe_from_single_blob()
      elif transcription_type == 1:
         transcription_definition = self.transcribe_with_custom_model()
      elif transcription_type == 2:
         transcription_definition = self.transcribe_from_container()
   
      print("Starting transcription client...")
            
      (created_transcription,status,headers,) = self.api.create_transcription_with_http_info(
          transcription=transcription_definition
      )

      # get the transcription Id from the location URI
      self.transcription_id = headers["location"].split("/")[-1]
      # Log information about the created transcription. If you should ask for support, please
      # include this information.
      print(
          f"Created new transcription with id '{self.transcription_id}' in region {self.azure_region}"
      )

   def check_transcription(self,wait_time=5,wait=False):
      print("Checking status.")
      completed = False
      while not completed:
         completed = not wait
         # wait for 5 seconds before refreshing the transcription status
         time.sleep(wait_time)
         transcription = self.api.get_transcription(self.transcription_id)
         print(f"Transcriptions status: {transcription.status}")
         if transcription.status in ("Failed", "Succeeded"):
             completed = True
         if transcription.status == "Succeeded":
             pag_files = self.api.get_transcription_files(self.transcription_id)
             for file_data in self._paginate(pag_files):
                 if file_data.kind != "Transcription":
                     continue
                 audiofilename = file_data.name
                 results_url = file_data.links.content_url
                 results = requests.get(results_url)
                 try:
                     contenido_blob = results.content.decode("utf-8")
                     data_json = json.dumps(contenido_blob)
                     path_transcription = os.path.join(self.json_folder_path,audiofilename)
                     with open(path_transcription, "w") as file:
                        file.write(data_json)
                     print(f"Results for {audiofilename}:\nOK")
                 except Exception as e:
                     print(f"Error en el blob {audiofilename}- Error:{e}")
         elif transcription.status == "Failed":
             print(f"Transcription failed: {transcription.properties.error.message}")
      

class AzureVendor(AzureTranscription):
   def __init__(
        self,
        storage_connection_string="",
        audios_folder_path="audios/",
        json_folder_path="transcriptions/",
        audio_container_name="",
        json_container_name="",
        *args,
        **kwargs,
   ):
      self.blob_service_client = BlobServiceClient.from_connection_string(
          storage_connection_string
      )
      self.audios_folder_path = audios_folder_path
      self.json_folder_path = json_folder_path
      self.audio_container_name = audio_container_name
      self.json_container_name = json_container_name
      if not os.path.exists(self.audios_folder_path):
          os.mkdir(self.audios_folder_path)
      if not os.path.exists(self.json_folder_path):
          os.mkdir(self.json_folder_path)
      super().__init__(*args, **kwargs)

   def upload_audio(self):
      archivos = os.listdir(self.audios_folder_path)
      for file_name in archivos:
         file_path = os.path.join(self.audios_folder_path, file_name)
         ruta_wav = transformar_audio_wav(file_path)
         file_wav = file_name.split(".")[0]
         blob_client = self.blob_service_client.get_blob_client(
             container=self.audio_container_name, blob=file_wav
         )
         with open(ruta_wav, "rb") as data:
             blob_client.upload_blob(data)
             print(f"Uploaded {ruta_wav}.")
   
   def list_blobs(self, audios=True, container=""):
      print("\nListing blobs...")
      # List the blobs in the container
      if container:
          current_container = current_container
      else:
          if audios:
              current_container = self.audio_container_name
          else:
              current_container = self.json_container_name
      container_client = self.blob_service_client.get_container_client(
          container=current_container
      )
      blob_list = container_client.list_blobs()
      for blob in blob_list:
         print("\t" + blob.name)
      return blob_list

   def download_file_json(self, local_file_name, blob):
      # Download the blob to a local file
      download_file_path = os.path.join(self.json_folder_path, local_file_name)
      container_client = self.blob_service_client.get_container_client(
          container=self.json_container_name
      )
      print("\nDownloading blob to \n\t" + download_file_path)

      with open(file=download_file_path, mode="wb") as download_file:
         download_file.write(container_client.download_blob(blob.name).readall())

   def download_all_json_files(self):
      container_client = self.blob_service_client.get_container_client(
          container=self.json_container_name
      )
      blob_list = container_client.list_blobs()
      for blob in blob_list:
          name = blob.name.split("/")[-1]
          self.download_file_json(name, blob)
