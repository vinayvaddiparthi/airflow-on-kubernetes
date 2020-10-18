from pathlib import Path

from equifax_extras.utils.batch import BaseRequestFile, BaseRow, FixedWidthColumn
from equifax_extras.data.models import Applicant, Address


class RequestFile(BaseRequestFile):
    class Row(BaseRow):
        separator = ""
        reference_number = FixedWidthColumn(12)
        last_name = FixedWidthColumn(25)
        first_name = FixedWidthColumn(15)
        middle_name = FixedWidthColumn(10)
        suffix = FixedWidthColumn(2)
        filler1 = FixedWidthColumn(15)
        sin = FixedWidthColumn(9)
        date_of_birth = FixedWidthColumn(6)
        civic_line = FixedWidthColumn(30)
        city = FixedWidthColumn(20)
        province_code = FixedWidthColumn(2)
        postal_code = FixedWidthColumn(6)
        account_number = FixedWidthColumn(18)
        applicant_guid = FixedWidthColumn(25)
        equifax_reserved_field = FixedWidthColumn(25)  # RESERVED, DO NOT USE

    def __init__(self, path: Path):
        super().__init__(path)
        self.encoding = "cp1252"

    def write_header(self) -> None:
        time_tag = self.timestamp.strftime("%Y%m%d%H%M%S")
        header = f"BHDR-EQUIFAX{time_tag}ADVITFINSCOREDA2\n"
        with open(self.path, "a+", encoding=self.encoding) as file:
            file.write(header)

    def write_footer(self) -> None:
        time_tag = self.timestamp.strftime("%Y%m%d%H%M%S")
        rows = str(self.rows).rjust(8, "0")
        trailer = f"BTRL-EQUIFAX{time_tag}ADVITFINSCOREDA2{rows}"
        with open(self.path, "a+", encoding=self.encoding) as file:
            file.write(trailer)

    def append(
        self, applicant: Applicant, address: Address, sfoi_account_id: str = ""
    ) -> None:
        reference_number = f"{applicant.id}".rjust(12, "0")
        last_name = applicant.last_name.strip()
        first_name = applicant.first_name.strip()
        middle_name = applicant.middle_name.strip()
        suffix = applicant.suffix.strip()
        sin = applicant.sin.strip() if applicant.sin else "000000000"
        date_of_birth = (
            applicant.date_of_birth.strftime("%m%d%y")
            if applicant.date_of_birth
            else ""
        )
        civic_line = " ".join(address.lines) if address else ""
        city = address.city if address else ""
        province_code = address.state_province if address else ""
        postal_code = address.postal_code if address else ""
        account_number = sfoi_account_id if sfoi_account_id else ""

        row = self.__class__.Row(
            reference_number=reference_number,
            last_name=last_name,
            first_name=first_name,
            middle_name=middle_name,
            suffix=suffix,
            sin=sin,
            date_of_birth=date_of_birth,
            civic_line=civic_line,
            city=city,
            province_code=province_code,
            postal_code=postal_code,
            account_number=account_number,
            applicant_guid=applicant.guid,
        )
        self.write(row)
