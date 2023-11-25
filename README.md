# charity_data
CRA charity data extract to MongoDB

----

Old code for finding directors worksheet url and financial information:
>
    # Get the directors worksheet url
    section_b_header = soup.find(
        lambda tag: tag.name == "summary"
        and "Section B - Directors/Trustees and Like Officials"
        in tag.get_text(strip=True),
    )  # Get the element for Section B details

    directors_worksheet_parent = section_b_header.find_parent(
        "details"
    )  # Get the parent element for section b header

    directors_worksheet = directors_worksheet_parent.find(
        "p"
    )  # Get element containing directors worksheet url

    directors_worksheet_url = directors_worksheet.find("a")[
        "href"
    ].strip()  # Get directors worksheet url

    # Get financial information url
    financial_info = soup.find(
        lambda tag: tag.name == "p"
        and "Schedule 6 - Detailed financial information"
        in tag.get_text(strip=True)
    )  # Get the div containing detailed financial info

    financial_info_url = financial_info.find("a")[
        "href"
    ].strip()  # Get the href to detailed financial info