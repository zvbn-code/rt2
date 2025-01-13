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

    dir_url = project_url + "dmsf.xml?folder_id=" + str(folder_id)
    print(dir_url)
    rdir = requests.get(dir_url, headers={"X-Redmine-API-Key": key_redmine}, timeout=10)
    # Löschen der Dateien im Echtzeitordner
    for child in ET.fromstring(rdir.text).iter("file"):
        for f in child.iter("id"):
            print(f.tag, f.text)
            del_url = f"https://vms.zvbn.de/dmsf/files/{f.text}.xml?commit=yes"
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
    return rdc
