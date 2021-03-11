from pathlib import Path
import csv

from equifax_extras.utils.batch import BaseRequestFile, BaseRow, Column
from equifax_extras.data.models import Merchant, Address, PhoneNumber


class RequestFile(BaseRequestFile):
    class Row(BaseRow):
        separator = ","
        reference_number = Column()
        business_name = Column()
        address = Column()
        city = Column()
        province = Column()
        postal_code = Column()
        phone = Column()
        fax = Column()
        file_number = Column()

    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self._output = open(self.path, "a+", encoding=self.encoding)

        column_names = self.__class__.Row.column_names
        self._writer = csv.DictWriter(self._output, fieldnames=column_names)

    def __del__(self) -> None:
        self._output.close()

    def write_header(self) -> None:
        self._writer.writeheader()

    def write_footer(self) -> None:
        pass

    def append(
        self, merchant: Merchant, address: Address, phone_number: PhoneNumber
    ) -> None:
        reference_number = merchant.guid
        business_name = merchant.name
        address_line = " ".join(address.lines) if address else ""
        city = address.city if address else ""
        province = address.state_province if address else ""
        postal_code = address.postal_code if address else ""
        phone = str(phone_number) if phone_number else ""
        fax = ""
        file_number = merchant.file_number

        row = self.__class__.Row(
            reference_number=reference_number,
            business_name=business_name,
            address=address_line,
            city=city,
            province=province,
            postal_code=postal_code,
            phone=phone,
            fax=fax,
            file_number=file_number,
        )
        data = row.column_data
        self._writer.writerow(data)
