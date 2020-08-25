from crawler_utils import write_to_file
from crawler_utils import update_collection
from crawler_utils import generate_random_string
from crawler_utils import update_collection_old
from crawler_utils import create_new_link
from bs4 import BeautifulSoup

def handle_applications(content_type):
    file_name = generate_random_string()
    if content_type[11:] == "/pdf":
        file_name += ".pdf"
    elif content_type[11:] == "/json":
        file_name += ".json"
    elif content_type[11:] == "/xml":
        file_name += ".xml"
    elif content_type[11:] == "/javascript":
        file_name += ".js"
    elif content_type[11:] == "/zip":
        file_name += ".zip"
    elif content_type[11:] == "/x-7z-compressed":
        file_name += ".7z"
    elif content_type[11:] == "/vnd.mozilla.xul+xml":
        file_name += ".xul"
    elif content_type[11:] == "/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        file_name += ".xlsx"
    elif content_type[11:] == "/vnd.ms-excel":
        file_name += ".xls"
    elif content_type[11:] == "/xhtml+xml":
        file_name += ".xhtml"
    elif content_type[11:] == "/x-tar":
        file_name += ".tar"
    elif content_type[11:] == "/x-sh":
        file_name += ".sh"
    elif content_type[11:] == "/rtf":
        file_name += ".rtf"
    elif content_type[11:] == "/vnd.rar":
        file_name += ".rar"
    elif content_type[11:] == "/vnd.openxmlformats-officedocument.presentationml.presentation":
        file_name += ".pptx"
    elif content_type[11:] == "/vnd.ms-powerpoint":
        file_name += ".ppt"
    elif content_type[11:] == "/x-httpd-php":
        file_name += ".php"
    # for other file types leave
    else:
        file_name = None
    return file_name


# handel content type of audio
def handle_audio(content_type):
    file_name = generate_random_string()
    if content_type[5:] == "/acc":
        file_name += ".acc"
    elif content_type[5:] == "/ogg":
        file_name += ".ogg"
    elif content_type[5:] == "/opus":
        file_name += ".ogg"
    elif content_type[5:] == "/wav":
        file_name += ".wav"
    elif content_type[5:] == "/webm":
        file_name += ".webm"
    elif content_type[5:] == "/3gpp":
        file_name += ".3gp"
    elif content_type[5:] == "/3gpp2":
        file_name += ".3g2"
    else:
        file_name = None
    return file_name


# handel content type of text
def handle_text(content_type):
    file_name = generate_random_string()
    if content_type[4:] == "/plain":
        temp = url.split(".")
        file_name += "." + temp[-1]
    elif content_type[4:] == "/xml":
        file_name += ".xml"
    elif content_type[4:] == "/javascript":
        file_name += ".xml"
    elif content_type[4:] == "/csv":
        file_name += ".xml"
    elif content_type[4:] == "/css":
        file_name += ".xml"
    elif content_type[4:] == "/css":
        file_name += ".xml"
    else:
        file_name = None
    return file_name


# handel content of type image
def handle_image(content_type):
    file_name = generate_random_string()
    if content_type[5:] == "/vnd.microsoft.icon":
        file_name += ".ico"
    elif content_type[5:] == "/jpeg":
        file_name += ".jpg"
    elif content_type[5:] == "/bmp":
        file_name += ".bmp"
    elif content_type[5:] == "/gif":
        file_name += ".gif"
    elif content_type[5:] == "/png":
        file_name += ".png"
    elif content_type[5:] == "/svg+xml":
        file_name += ".svg"
    elif content_type[5:] == "/tiff":
        file_name += ".tiff"
    elif content_type[5:] == "/webp":
        file_name += ".webp"
    else:
        file_name = None
    return file_name


# handel videos
def handle_video(content_type):
    file_name = generate_random_string()
    if content_type[5:] == "/x-msvideo":
        file_name += ".avi"
    elif content_type[5:] == "/mpeg":
        file_name += ".mpeg"
    elif content_type[5:] == "/ogg":
        file_name += ".ogv"
    elif content_type[5:] == "/mp2t":
        file_name += ".ts"
    elif content_type[5:] == "/webm":
        file_name += ".webm"
    elif content_type[5:] == "/3gpp":
        file_name += ".3gp"
    elif content_type[5:] == "/3gpp2":
        file_name += ".3g2"
    else:
        file_name = None
    return file_name


# hadel types other than html
def other_content_types(url, collection, http_status, content_length, content_type, html_text, new=True):
    file_name = None

    # if it is application
    if content_type[:11] == "application":
        file_name = handle_applications(content_type)

    # if content type is audio
    elif content_type[:5] == "audio":
        file_name = handle_audio(content_type)

    # if content type is text
    elif content_type[:4] == "text":
        file_name = handle_text(content_type)

    # if content type is image
    elif content_type[:5] == "image":
        file_name = handle_image(content_type)

    # if content type is video
    elif content_type[:5] == "video":
        file_name = handle_video(content_type)

    if file_name != None:
        file_name = write_to_file(file_name, html_text)
        # for updating not crawled data new is true and for crawling old data new is false
        if new:
            update_collection(url=url, http_status=http_status, content_length=content_length,
                              content_type=content_type, file_name=file_name, collection=collection)
        else:
            update_collection_old(url=url, http_status=http_status, content_length=content_length,
                                  content_type=content_type, file_name=file_name, collection=collection)

# handel html
def handle_html(url, html_text, http_status, collection, content_type, content_length, new=True):
    soup = BeautifulSoup(html_text, 'html.parser')
    a_tags = soup.find_all("a")
    for tag in a_tags:
        link = tag.get("href")
        # clean link
        try:
            # if link is not http(or https) or starts with / it is not valid
            if link[:4] != "http" and link[0] != "/":
                # print(link,"is not a valid link")
                continue
            if link[0] == "/":
                link = url + link
            # links like example.com and example.com/ are same
            if link[-1] == "/":
                link = link[:-1]
        # if link is empty string
        except (IndexError, TypeError):
            # print(link,"is not a valid link")
            continue
        # if link not present in data base then add it
        if collection.find_one({"link": link}) == None:
            create_new_link(url, link, collection)

    file_name = generate_random_string()
    file_name += ".html"
    file_name = write_to_file(file_name, html_text)

    # for updating not crawled data new is true and for crawling old data new is false
    if new:
        update_collection(url=url, http_status=http_status, content_length=content_length, content_type=content_type,
                          file_name=file_name, collection=collection)
    else:
        update_collection_old(url=url, http_status=http_status, content_length=content_length,
                              content_type=content_type, file_name=file_name, collection=collection)
