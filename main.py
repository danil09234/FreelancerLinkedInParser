import re
from typing import NamedTuple, Union, Coroutine, Any
import csv
import asyncio
import aiopath
import lxml.etree


class DataRow(NamedTuple):
    full_name: str | None
    job_title: str | None
    company: str | None
    email: str | None
    description: str | None
    date: str | None
    number_of_comments: int | None
    url: str | None


async def save_data_rows_to_csv(filename: aiopath.AsyncPath):
    async with filename.open('w', newline='') as file:
        writer = csv.writer(file)

        await writer.writerow(
            ['FullName', 'Job Title', 'Company', 'Email', 'Description', 'Date', 'No. Comments', 'Url']
        )

        while True:

            data_rows = yield
            if data_rows is None:
                print("All data written")
                break
            print("Writing parsed data...")

            for row in data_rows:
                await writer.writerow(row)
        raise StopAsyncIteration()


def get_posts_trees(tree: lxml.etree) -> list:
    posts = tree.xpath("//div[contains(@class, 'artdeco-card') and .//h2[contains(@class, 'visually-hidden')]]")

    return posts


def get_inner_text(element: Union[lxml.etree.Element, lxml.etree.ElementTree]) -> str:
    text = element.text or ''
    for child in element:
        text += get_inner_text(child)
        if child.tail:
            text += child.tail
    return text.strip()


def extract_email(text: str) -> str | None:
    email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"
    match = re.search(email_pattern, text)
    if match:
        return match.group()
    else:
        return None


FULL_NAME_XPATH = ".//span[contains(@class, 'update-components-actor__name')]//span[@dir='ltr']"
JOB_TITLE_XPATH = ".//span[contains(@class, 'update-components-actor__description')]//span[@aria-hidden='true']"
DATE_XPATH = ".//span[contains(@class, 'update-components-actor__sub-description')]//span[@class='visually-hidden']"
DESCRIPTION_XPATH = ".//div[@class='update-components-text relative feed-shared-update-v2__commentary ']"
URL_XPATH = \
    ".//a[contains(@class, 'app-aware-link') and contains(@class, 'update-components-actor__container-link')]/@href"
COMMENTS_NUMBER_XPATH = \
    ".//li[contains(@class, 'social-details-social-counts__comments')]/button/span[@aria-hidden='true']/text()"


def parse_html_soup(tree: lxml.etree) -> list[DataRow]:
    data_rows = []
    for post in get_posts_trees(tree):
        full_name = post.xpath(FULL_NAME_XPATH)[0].text
        job_title = post.xpath(JOB_TITLE_XPATH)[0].text
        date_field = post.xpath(DATE_XPATH)[0].text
        date = re.search(r"[1-9]\d?\w\w?", date_field).group()

        try:
            description_element = post.xpath(DESCRIPTION_XPATH)[0]
            description = get_inner_text(description_element)
        except IndexError:
            description = None

        if description is not None:
            email = extract_email(description)
        else:
            email = None

        url = post.xpath(URL_XPATH)[0]

        number_of_comments_field = post.xpath(COMMENTS_NUMBER_XPATH)[0]
        number_of_comments = int(re.search(r'[\d,]+', number_of_comments_field).group().replace(',', ''))

        data_rows.append(
            DataRow(
                full_name=full_name,
                job_title=job_title,
                company=None,
                email=email,
                description=description,
                date=date,
                number_of_comments=number_of_comments,
                url=url
            )
        )

    return data_rows


async def get_html_tree(filepath: aiopath.AsyncPath) -> lxml.etree:
    async with filepath.open(mode='r') as file_pointer:
        content = await file_pointer.read()
        tree = lxml.etree.fromstring(content, lxml.etree.HTMLParser())

    return tree


async def parse_html_file(filepath: aiopath.AsyncPath) -> list[DataRow]:
    if not await filepath.is_file():
        raise ValueError("Path is not a file!")
    if filepath.suffix != ".html":
        raise ValueError("File extension is invalid!")

    tree = await get_html_tree(filepath)
    result = parse_html_soup(tree)
    return result


SENTINEL = object()


async def send_data_to_csv_consumer(queue: asyncio.Queue, sender_function):
    while True:
        data = await queue.get()

        if data is SENTINEL:
            try:
                await sender_function.asend(None)
            except RuntimeError:
                return

        await sender_function.asend(data)


async def send_data_to_csv_producer(parser_function: Coroutine[Any, Any, list[DataRow]], queue: asyncio.Queue):
    data = await parser_function
    print("File processed")
    await queue.put(data)


async def parse_folder(folder_path: aiopath.AsyncPath, output_filename: aiopath.AsyncPath):
    if not await folder_path.is_dir():
        raise ValueError("Path is not a directory!")
    if await output_filename.exists():
        raise ValueError("Output filename exists!")

    data_write_queue = asyncio.Queue()
    sender_function = save_data_rows_to_csv(output_filename)
    await sender_function.asend(None)

    parse_file_tasks = [
        send_data_to_csv_producer(
            parse_html_file(file),
            data_write_queue
        ) async for file in folder_path.iterdir()
    ]

    sender_task = asyncio.create_task(send_data_to_csv_consumer(data_write_queue, sender_function))

    await asyncio.gather(*parse_file_tasks)

    await data_write_queue.put(SENTINEL)

    try:
        await sender_task
    except StopAsyncIteration:
        pass

if __name__ == '__main__':
    folder = aiopath.AsyncPath("Inputfolder")
    output = aiopath.AsyncPath("result.csv")

    asyncio.run(parse_folder(folder, output))
