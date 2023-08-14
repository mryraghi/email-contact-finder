import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import tldextract
import socket
from tqdm import tqdm
import sys
from multiprocessing import Pool, cpu_count
from urllib.parse import urlparse
import urllib.parse
import xml.etree.ElementTree as ET

# from llm import get_contact_url_from_list_of_urls

# remove SSL warnings
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537"
}


def fetch_url(url):
    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()  # This will raise an HTTPError if the HTTP request returned an unsuccessful status code
        return response
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


def is_valid_xml(sitemap_url):
    response = fetch_url(sitemap_url)
    if response:
        try:
            # Attempt to parse the content as XML
            ET.ElementTree(ET.fromstring(response.content))
            return True
        except ET.ParseError:
            print(f"Error parsing XML for {sitemap_url}")
            return False
    return False


def get_sitemap_link(website, depth=0):
    if depth > 3:
        # Prevent infinite recursion by limiting depth
        return None

    # Ensure the website URL is correctly formed
    parsed = urlparse(website)
    if not parsed.scheme:
        website = "http://" + website

    try:
        response = requests.get(
            urllib.parse.urljoin(website, "/robots.txt"), headers=headers, verify=False
        )
    except (
        requests.exceptions.RequestException,
        socket.gaierror,
        requests.exceptions.SSLError,
    ) as e:
        return None

    if response.status_code == 200:
        match = re.search(r"Sitemap:\s*(.*)", response.text, re.I)
        if match:
            sitemap_url = match.group(1)

            # The following line replaces double "https://" with a single one
            sitemap_url = sitemap_url.replace("https://https://", "https://")
            print("sitemap_url 1", sitemap_url)

            # Ensure the sitemap_url has a proper scheme
            parsed_url = urlparse(sitemap_url)
            if not parsed_url.scheme:
                parsed_url = parsed_url._replace(scheme="http")
                sitemap_url = parsed_url.geturl()

            # Check if the URL is a sitemap index
            if sitemap_url.endswith("xml"):
                try:
                    response = requests.get(sitemap_url, headers=headers, verify=False)
                except (
                    requests.exceptions.RequestException,
                    socket.gaierror,
                    requests.exceptions.SSLError,
                ) as e:
                    return None

                soup = BeautifulSoup(response.content, "lxml-xml")
                if soup.find("sitemapindex"):
                    # If it is, recursively fetch the first sitemap URL
                    sitemaps = soup.find_all("sitemap")
                    if sitemaps:
                        sitemap_url = sitemaps[0].loc.string
                        # Ensure the sitemap_url from sitemap index is absolute URL
                        parsed_sitemap_url = urlparse(sitemap_url)
                        if not parsed_sitemap_url.scheme:
                            sitemap_url = urllib.parse.urljoin(website, sitemap_url)
                        return get_sitemap_link(sitemap_url, depth=depth + 1)
            return sitemap_url
        else:
            # try to access sitemap.xml directly
            sitemap_url = urllib.parse.urljoin(website, "/sitemap.xml")
            if is_valid_xml(sitemap_url):
                print("sitemap_url works 2", sitemap_url)
                return sitemap_url
            else:
                print("sitemap_url do not work 2", sitemap_url)
                return None
    return None


def get_contact_link(website):
    # if sitemap is defined
    # if sitemap is not None:
    #     try:
    #         response = requests.get(sitemap, headers=headers, verify=False)
    #         if response.status_code == 200:
    #             soup = BeautifulSoup(response.text, "lxml-xml")
    #             links = soup.find_all("loc")
    #             link_texts = "\n- ".join([link.text for link in links])

    #             # print("links", get_contact_url_from_list_of_urls(link_texts))

    #             for link in links:
    #                 if "contact" in link.text:
    #                     submit_contact_form(link.text)
    #                     return link.text
    #                 elif link.text.endswith(".xml"):  # if the link is another sitemap
    #                     return get_contact_link(link.text, website)
    #     except (
    #         requests.exceptions.RequestException,
    #         socket.gaierror,
    #         requests.exceptions.SSLError,
    #     ) as e:
    #         print("")
    #         # continue to try /contact and /contact-us directly

    # try /contact and /contact-us directly
    for page in [
        "contact",
        "contact-us",
        "contacts",
        "contactus",
        "contactus.html",
        "contact-us.html",
        "contacts.html",
        "contactus.php",
        "contact-us.php",
        "contacts.php",
        "contactus.asp",
        "contact-us.asp",
        "contacts.asp",
    ]:
        for protocol in ["http", "https"]:
            url = f"{protocol}://{website}/{page}"
            try:
                response = requests.get(url, headers=headers, verify=False)
                if response.status_code == 200:
                    return url
            except (
                requests.exceptions.RequestException,
                socket.gaierror,
                requests.exceptions.SSLError,
            ) as e:
                continue

    return None


def get_contact_info(contact_page):
    try:
        response = requests.get(contact_page, headers=headers, verify=False)
    except (
        requests.exceptions.RequestException,
        socket.gaierror,
        requests.exceptions.SSLError,
    ) as e:
        return []

    if response.status_code == 200:
        domain = urlparse(contact_page).netloc

        # remove subdomain
        domain = ".".join(domain.split(".")[-2:])

        # search for any email-like text
        email = re.findall(
            r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b", response.text
        )
        obfuscated_email = re.findall(
            r"\b[A-Za-z0-9._%+-]+\s*\[\s*at\s*\]\s*[A-Za-z0-9.-]+\s*\[\s*dot\s*\]\s*[A-Za-z0-9.-]+\b",
            response.text,
        )

        # lowercase all emails
        email = [e.lower() for e in email]
        obfuscated_email = [e.lower() for e in obfuscated_email]

        # filter out any email from a different domain
        email = [e.lower() for e in email if e.split("@")[-1] == domain]
        obfuscated_email = [
            e
            for e in obfuscated_email
            if e.replace(" [at] ", "@").replace(" [dot] ", ".").split("@")[-1] == domain
        ]
        print(email, domain)

        # merge the two lists
        email.extend(obfuscated_email)

        return email if email else []

    return []


def submit_contact_form(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Look for the contact us form (this may vary greatly depending on the specific HTML structure)
    contact_us_form = None
    for form in soup.find_all("form"):
        if (
            "contact" in form.get("action", "").lower()
            or "contact" in str(form).lower()
        ):
            contact_us_form = form
            break

    if contact_us_form:
        # Extract form details (this will depend on the specific inputs and their types)
        input_fields = contact_us_form.find_all("input")
        for input_field in input_fields:
            if input_field.get("type") == "hidden":
                break
            print(f"Name: {input_field.get('name')}, Type: {input_field.get('type')}")

        # You would then take this information and construct a prompt for OpenAI,
        # receive the response, fill in the form accordingly, and submit it using a tool like Selenium.
    else:
        print("Contact Us form not found")


def process_website(row):
    website = row["Website-href"]

    extracted = tldextract.extract(str(website))
    domain_name = (
        f"http://{extracted.subdomain}.{extracted.domain}.{extracted.suffix}"
        if extracted.subdomain
        else f"http://{extracted.domain}.{extracted.suffix}"
    )
    naked_domain_name = (
        f"{extracted.subdomain}.{extracted.domain}.{extracted.suffix}"
        if extracted.subdomain
        else f"{extracted.domain}.{extracted.suffix}"
    )

    addresses = get_contact_info(domain_name)
    # sitemap_link = get_sitemap_link(domain_name)
    contact_link = get_contact_link(naked_domain_name)

    if contact_link:
        other_addresses = get_contact_info(contact_link)
        addresses.extend(other_addresses)

    # remove duplicates
    addresses = list(set(addresses))
    print("=> addresses", addresses)
    row["contact_email"] = ";".join(addresses)
    return row


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <input_csv>")
        exit(1)

    df = pd.read_csv(sys.argv[1])

    # Prepare the output file and write the headers
    output_file = f"{sys.argv[1].split('.')[0]}_output.csv"
    with open(output_file, "w") as f:
        f.write(",".join(df.columns) + ",contact_email\n")
    # Assuming df.columns doesn't include 'contact_email'

    # Create a multiprocessing Pool
    with Pool(processes=cpu_count()) as pool:
        for processed_row in tqdm(
            pool.imap(process_website, df.to_dict("records")),
            total=len(df),
            desc="Processing websites",
            unit="website",
        ):
            print(processed_row)
            # Write processed email to the output file
            with open(output_file, "a") as f:
                f.write(",".join(map(str, processed_row.values())) + "\n")
