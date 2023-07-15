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
    raise StopAsyncIteration


def get_posts_trees(tree: lxml.etree) -> list:
    posts = tree.xpath("//div[contains(@class, 'artdeco-card') and .//h2[contains(@class, 'visually-hidden')]]")

    return posts


def get_inner_text(element: Union[lxml.etree.Element, lxml.etree.ElementTree]) -> str:
    text = element.text or ''
    for child in element:
        text += " " + get_inner_text(child)
        if child.tail:
            text += " " + child.tail
    return text.strip()


EMAIL_REGEX = None
try:
    with open("email_regex.txt", "r") as read_file:
        EMAIL_REGEX = read_file.read()
except FileNotFoundError:
    raise RuntimeError("File with regex not found!")


def extract_email(text: str) -> str | None:
    email_pattern = EMAIL_REGEX
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
        try:
            full_name = post.xpath(FULL_NAME_XPATH)[0].text
        except IndexError:
            continue

        try:
            job_title = post.xpath(JOB_TITLE_XPATH)[0].text
        except IndexError:
            job_title = None

        try:
            date_field = post.xpath(DATE_XPATH)[0].text
        except IndexError:
            date = None
        else:
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

        try:
            number_of_comments_field = post.xpath(COMMENTS_NUMBER_XPATH)[0]
        except IndexError:
            number_of_comments = None
        else:
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
            except StopAsyncIteration:
                pass
            finally:
                return

        await sender_function.asend(data)


async def send_data_to_csv_producer(parser_function: Coroutine[Any, Any, list[DataRow]], queue: asyncio.Queue):
    data = await parser_function
    print("File processed")
    await queue.put(data)


async def get_output_file() -> aiopath.AsyncPath:
    index = 0
    while True:
        result = aiopath.AsyncPath(f"result_{index}.csv")
        if not await result.exists():
            return result
        index += 1


async def parse_folder(folder_path: aiopath.AsyncPath):
    if not await folder_path.is_dir():
        raise ValueError("Path is not a directory!")
    output_filename = await get_output_file()

    data_write_queue = asyncio.Queue()
    sender_function = save_data_rows_to_csv(output_filename)
    await sender_function.asend(None)

    parse_file_tasks = [
        send_data_to_csv_producer(
            parse_html_file(file),
            data_write_queue
        ) async for file in folder_path.iterdir()
    ]

    consumer_task = asyncio.create_task(send_data_to_csv_consumer(data_write_queue, sender_function))

    await asyncio.gather(*parse_file_tasks)
    await data_write_queue.put(SENTINEL)
    await consumer_task


if __name__ == '__main__':
    folder = aiopath.AsyncPath("Inputfolder2")

    asyncio.run(parse_folder(folder))
