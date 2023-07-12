import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import tldextract
import socket
from tqdm import tqdm
import sys
from multiprocessing import Pool, cpu_count

# remove SSL warnings
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


import urllib.parse


def get_sitemap_link(website, depth=0):
    if depth > 3:
        # Prevent infinite recursion by limiting depth
        return None

    try:
        response = requests.get(
            urllib.parse.urljoin(website, "/robots.txt"), verify=False
        )
    except (requests.exceptions.RequestException, socket.gaierror) as e:
        return None

    if response.status_code == 200:
        match = re.search(r"Sitemap:\s*(.*)", response.text, re.I)
        if match:
            sitemap_url = match.group(1)
            # Check if the URL is a sitemap index
            if sitemap_url.endswith("xml"):
                response = requests.get(sitemap_url, verify=False)
                soup = BeautifulSoup(response.content, "lxml-xml")
                if soup.find("sitemapindex"):
                    # If it is, recursively fetch the first sitemap URL
                    sitemaps = soup.find_all("sitemap")
                    if sitemaps:
                        sitemap_url = sitemaps[0].loc.string
                        return get_sitemap_link(sitemap_url, depth=depth + 1)
            return sitemap_url
        else:
            # try to access sitemap.xml directly
            try:
                sitemap_url = urllib.parse.urljoin(website, "/sitemap.xml")
                response = requests.get(sitemap_url, verify=False)
                if response.status_code == 200:
                    return sitemap_url
            except (requests.exceptions.RequestException, socket.gaierror) as e:
                return None
    return None


def get_contact_link(sitemap, website):
    if sitemap is None:
        # try /contact and /contact-us directly
        for page in ["contact", "contact-us"]:
            try:
                response = requests.get(f"{website}/{page}", verify=False)
                if response.status_code == 200:
                    return f"{website}/{page}"
            except (requests.exceptions.RequestException, socket.gaierror) as e:
                return None
        return None

    try:
        response = requests.get(sitemap, verify=False)
    except (requests.exceptions.RequestException, socket.gaierror) as e:
        return None

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "lxml-xml")
        links = soup.find_all("loc")

        for link in links:
            if "contact" in link.text:
                return link.text
            elif link.text.endswith(".xml"):  # if the link is another sitemap
                return get_contact_link(link.text, website)
    return None


def get_contact_info(contact_page):
    try:
        response = requests.get(contact_page, verify=False)
    except (requests.exceptions.RequestException, socket.gaierror) as e:
        return None

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "lxml")
        # search for any email-like text
        email = re.findall(
            r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b", soup.text
        )
        obfuscated_email = re.findall(
            r"\b[A-Za-z0-9._%+-]+\s*\[\s*at\s*\]\s*[A-Za-z0-9.-]+\s*\[\s*dot\s*\]\s*[A-Za-z0-9.-]+\b",
            soup.text,
        )

        if email:
            return email[0]
        elif obfuscated_email:
            return obfuscated_email[0].replace(" [at] ", "@").replace(" [dot] ", ".")
    return None


def process_website(website):
    extracted = tldextract.extract(str(website))
    domain_name = (
        f"http://{extracted.subdomain}.{extracted.domain}.{extracted.suffix}"
        if extracted.subdomain
        else f"http://{extracted.domain}.{extracted.suffix}"
    )

    sitemap_link = get_sitemap_link(domain_name)
    contact_link = get_contact_link(sitemap_link, domain_name)

    if contact_link:
        contact_info = get_contact_info(contact_link)
        return contact_info
    else:
        return None


email_list = []

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <input_csv>")
        exit(1)

    df = pd.read_csv(sys.argv[1])

    # Create a multiprocessing Pool
    with Pool(processes=cpu_count()) as pool:
        email_list = list(
            tqdm(
                pool.imap(process_website, df["website"]),
                total=len(df["website"]),
                desc="Processing websites",
                unit="website",
            )
        )

    df["contact_email"] = email_list
    df.to_csv(f"{sys.argv[1].split('.')[0]}_output.csv", index=False)
