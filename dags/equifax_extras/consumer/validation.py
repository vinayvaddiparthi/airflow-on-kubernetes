from typing import Any, Dict, IO
import re


def check_len(line: str) -> Any:
    if len(line) != 221:
        return len(line)
    return None


def convert_line(line: str) -> list:
    format_table = {
        "customer_reference_number": 12,
        "last_name": 25,
        "first_name": 15,
        "middle_name": 10,
        "suffix": 2,
        "filler1": 15,
        "sin": 9,
        "dob_text": 6,
        "address": 30,
        "city": 20,
        "province": 2,
        "postal_code": 6,
        "account_number": 18,  # 15
        "filler2": 25,  # 28
    }
    indices = generate_list(0, format_table)
    result = [line[i:j] for i, j in zip(indices, indices[1:] + [None])]
    return result


def generate_list(start: int, dol: dict) -> list:
    result = [start]
    for l in dol:
        result.append(result[-1] + dol[l])
    return result[:-1]


def check_sin(sin: str) -> Any:
    if not sin.isdigit() and sin != "         ":
        return sin
    return None


def check_dob(dob: str) -> Any:
    month = dob[:2]
    day = dob[2:4]
    if not dob.isdigit():
        return dob
    if int(month) > 12 or int(month) < 1:
        return dob
    if int(day) > 31 or int(day) < 1:
        return dob
    return None


def check_city(city: str) -> Any:
    if not city.strip(" "):
        return city
    return None


def check_province(province: str) -> Any:
    if not province.isalpha():
        return province
    return None


def check_postal_code(code: str) -> Any:
    if not re.match("[a-zA-Z]\\d[a-zA-Z]\\d[a-zA-Z]\\d", code) and code != "      ":
        return code
    return None


def check_header(header: str) -> Any:
    if len(header) != 43:
        return f"length error: {len(header)}"
    if not header.startswith("BHDR-EQUIFAX"):
        return f"prefix error: {header}"
    if not header.endswith("ADVITFINSCOREDA2\n"):
        return f"suffix error: {header}"
    return None


def check_footer(footer: str, count: int) -> Any:
    if len(footer) != 50:
        return f"length error: {len(footer)}"
    if not footer.startswith("BTRL-EQUIFAX"):
        return f"prefix error: {footer}"
    if not footer[:-8].endswith("ADVITFINSCOREDA2"):
        return f"suffix error: {footer}"
    if int(footer[-8:].lstrip("0")) != count:
        return f"count error: {int(footer[-8:].lstrip('0'))}"
    return None


def validate(
    file: IO,
) -> None:
    error: Dict[str, Any] = {
        "length": [],
        "header": [],
        "footer": [],
        "dob": [],
        "city": [],
        "province": [],
        "postal": [],
        "sin": [],
    }
    lines = file.readlines()
    header = lines[0]
    footer = lines[len(lines) - 1]
    lines = lines[1:-1]
    if check_header(header):
        error["header"].append(check_header(header))
    if check_footer(footer, len(lines)):
        error["footer"].append(check_footer(footer, len(lines)))
    for line in lines:
        if check_len(line):
            error["length"].append(check_len(line))

        data = convert_line(line)
        if check_sin(data[6]):
            error["sin"].append({data[13].rstrip("\n").rstrip(" "): check_sin(data[6])})
        if check_dob(data[7]):
            error["dob"].append({data[13].rstrip("\n").rstrip(" "): check_dob(data[7])})
        if check_city(data[9]):
            error["city"].append(
                {data[13].rstrip("\n").rstrip(" "): check_dob(data[9])}
            )
        if check_province(data[10]):
            error["province"].append(
                {data[13].rstrip("\n").rstrip(" "): check_dob(data[10])}
            )
        if check_postal_code(data[11]):
            error["postal"].append(
                {data[13].rstrip("\n").rstrip(" "): check_dob(data[11])}
            )
    print(error)
    for key in error:
        if error[key]:
            print(f"{key} errors: {len(error[key])}")
