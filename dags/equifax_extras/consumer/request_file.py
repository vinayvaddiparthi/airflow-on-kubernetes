from datetime import datetime

from equifax_extras.utils.batch.fixed_width_column import FixedWidthColumn
from equifax_extras.utils.batch.base_row import BaseRow
from equifax_extras.utils.batch.base_request_file import BaseRequestFile
from equifax_extras.models.core import Loan
from equifax_extras.models.kyc import Applicant


class RequestFile(BaseRequestFile):
    class Row(BaseRow):
        reference_number = FixedWidthColumn(12)
        last_name = FixedWidthColumn(25)
        first_name = FixedWidthColumn(15)
        middle_name = FixedWidthColumn(10)
        suffix = FixedWidthColumn(2)
        p0 = FixedWidthColumn(15)
        sin = FixedWidthColumn(9)
        date_of_birth = FixedWidthColumn(6)
        street_number_thoroughfare = FixedWidthColumn(30)
        city = FixedWidthColumn(20)
        province_code = FixedWidthColumn(2)
        postal_code = FixedWidthColumn(6)
        account_number = FixedWidthColumn(18)
        p1 = FixedWidthColumn(50)

    def append(self, applicant: Applicant, loan: Loan) -> None:
        reference_number = f"{applicant.id}".rjust(12, "0")

        last_name = str(applicant.last_name, "utf-8")
        first_name = str(applicant.first_name, "utf-8")
        middle_name = str(applicant.middle_name, "utf-8")
        suffix = applicant.attribute("suffix")

        sin = applicant.attribute("sin")
        if not sin:
            sin = "000000000"

        dob_str = str(applicant.date_of_birth, "utf-8")
        dob = datetime.strptime(dob_str, "%Y-%m-%d")
        date_of_birth = dob.strftime("%m%d%y")

        physical_address = applicant.physical_address
        if physical_address:
            street_number_thoroughfare = physical_address.civic_line
            city = physical_address.city
            province_code = physical_address.province
            postal_code = physical_address.postal_code
        else:
            street_number_thoroughfare = ""
            city = ""
            province_code = ""
            postal_code = ""

        sfoi_account_id = loan.sfoi_account_id
        account_number = sfoi_account_id if sfoi_account_id else ""

        row = RequestFile.Row(
            reference_number=reference_number,
            last_name=last_name,
            first_name=first_name,
            middle_name=middle_name,
            suffix=suffix,
            sin=sin,
            date_of_birth=date_of_birth,
            street_number_thoroughfare=street_number_thoroughfare,
            city=city,
            province_code=province_code,
            postal_code=postal_code,
            account_number=account_number,
        )
        self.write(row)
