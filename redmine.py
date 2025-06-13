import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
import os

load_dotenv()

def delete_upload_dmsf(
    project_url: str,
    folder_id: int,
    folder_file_name: str, 
    file_name: str,
    key_redmine=None
) -> None:
    """Upload von Dateien in das Redmine Projekt ZVBN Steuerungsprojekt"""
    if key_redmine is None:
        key_redmine = os.getenv("REDMINE_API_KEY")

    dir_url = f"{project_url}dmsf.xml?folder_id={folder_id}"
    print(dir_url)
    r = requests.get(dir_url, headers={"X-Redmine-API-Key": key_redmine}, timeout=10)
    root = ET.fromstring(r.text)
    # Löschen der Dateien im Echtzeitordner
    for file_element in root.findall("dmsf_nodes/node", namespaces=None): #dmsf ab Version 3.2.4 12.06.2025
        #print(f"File Element: {file_element.tag} Text: {file_element.text}")
        file_id_elem = file_element.find("id")
        file_title_elem = file_element.find("title")
        file_type_elem = file_element.find("type")
        file_filename_elem = file_element.find("filename")

        file_id = file_id_elem.text if file_id_elem is not None else None
        file_title = file_title_elem.text if file_title_elem is not None else None
        file_type = file_type_elem.text if file_type_elem is not None else None
        file_filename = file_filename_elem.text if file_filename_elem is not None else None

        print(f"ID: {file_id}, Title: {file_title} Type: {file_type} Filename: {file_filename}")

        del_url = f"https://vms.zvbn.de/dmsf/files/{file_id}.xml?commit=yes"
        
        rdel = requests.delete(
            del_url, headers={"X-Redmine-API-Key": key_redmine}, timeout=10
        )
        print(rdel)

    # Upload der Datei und Abfragen des Tokens
    url_upload = f"{project_url}dmsf/upload.xml?filename={file_name}"
    print(url_upload)

    with open(folder_file_name, "rb") as file_input:
        rdu = requests.post(
            url_upload,
            headers={
                "X-Redmine-API-Key": key_redmine,
                "Content-Type": "application/octet-stream",
            },
            data=file_input,
            timeout=10,
        )

    for child in ET.fromstring(rdu.text).findall("token"):
        print(child.tag, child.text)
    # Erzeugen der Commit XML

    xml_commit = f""" <?xml version="1.0" encoding="utf-8" ?>
    <attachments>
      <folder_id>{folder_id}</folder_id>
      <uploaded_file>
        <name>{file_name}</name>
        <title>{file_name}</title>
        <description>REST API</description>
        <version>3</version> <!-- It must be 3 (Custom version) -->
        <custom_version_major>1</custom_version_major> <!-- Major version -->
        <custom_version_minor>0</custom_version_minor>
        <comment>From API</comment>
        <!-- For an automatic version: -->
        <version/>
        <token>{child.text}</token>
      </uploaded_file>
    </attachments>"""
    print(xml_commit)
    url4 = project_url + "dmsf/commit.xml"
    print(url4)
    rdc = requests.post(
        url4,
        headers={"X-Redmine-API-Key": key_redmine, "Content-Type": "application/xml"},
        data=xml_commit,
        timeout=10,
    )
    print(rdc)
