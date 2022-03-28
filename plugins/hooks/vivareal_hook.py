import json
import time
import logging
import requests

from backoff import on_exception, expo
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class VivarealHook(BaseHook):

    def __init__(self) -> None:
        self.url = "https://glue-api.vivareal.com/v2/listings?addressCity=Santos&addressLocationId=BR>Sao Paulo>NULL>Santos&addressNeighborhood=&addressState=São Paulo&addressCountry=Brasil&addressStreet=&addressZone=&addressPointLat=&addressPointLon=&business=SALE&facets=amenities&unitTypes=APARTMENT&unitSubTypes=UnitSubType_NONE,DUPLEX,LOFT,STUDIO,TRIPLEX&unitTypesV3=APARTMENT&usageTypes=RESIDENTIAL&listingType=USED&parentId=null&categoryPage=RESULT&includeFields=search(result(listings(listing(displayAddressType,amenities,usableAreas,constructionStatus,listingType,description,title,unitTypes,nonActivationReason,propertyType,unitSubTypes,id,portal,parkingSpaces,address,suites,publicationType,externalId,bathrooms,usageTypes,totalAreas,advertiserId,bedrooms,pricingInfos,showPrice,status,advertiserContact,videoTourLink,whatsappNumber,stamps),account(id,name,logoUrl,licenseNumber,showAddress,legacyVivarealId,phones),medias,accountLink,link)),totalCount),page,seasonalCampaigns,fullUriFragments,nearby(search(result(listings(listing(displayAddressType,amenities,usableAreas,constructionStatus,listingType,description,title,unitTypes,nonActivationReason,propertyType,unitSubTypes,id,portal,parkingSpaces,address,suites,publicationType,externalId,bathrooms,usageTypes,totalAreas,advertiserId,bedrooms,pricingInfos,showPrice,status,advertiserContact,videoTourLink,whatsappNumber,stamps),account(id,name,logoUrl,licenseNumber,showAddress,legacyVivarealId,phones),medias,accountLink,link)),totalCount)),expansion(search(result(listings(listing(displayAddressType,amenities,usableAreas,constructionStatus,listingType,description,title,unitTypes,nonActivationReason,propertyType,unitSubTypes,id,portal,parkingSpaces,address,suites,publicationType,externalId,bathrooms,usageTypes,totalAreas,advertiserId,bedrooms,pricingInfos,showPrice,status,advertiserContact,videoTourLink,whatsappNumber,stamps),account(id,name,logoUrl,licenseNumber,showAddress,legacyVivarealId,phones),medias,accountLink,link)),totalCount)),account(id,name,logoUrl,licenseNumber,showAddress,legacyVivarealId,phones,phones),developments(search(result(listings(listing(displayAddressType,amenities,usableAreas,constructionStatus,listingType,description,title,unitTypes,nonActivationReason,propertyType,unitSubTypes,id,portal,parkingSpaces,address,suites,publicationType,externalId,bathrooms,usageTypes,totalAreas,advertiserId,bedrooms,pricingInfos,showPrice,status,advertiserContact,videoTourLink,whatsappNumber,stamps),account(id,name,logoUrl,licenseNumber,showAddress,legacyVivarealId,phones),medias,accountLink,link)),totalCount)),owners(search(result(listings(listing(displayAddressType,amenities,usableAreas,constructionStatus,listingType,description,title,unitTypes,nonActivationReason,propertyType,unitSubTypes,id,portal,parkingSpaces,address,suites,publicationType,externalId,bathrooms,usageTypes,totalAreas,advertiserId,bedrooms,pricingInfos,showPrice,status,advertiserContact,videoTourLink,whatsappNumber,stamps),account(id,name,logoUrl,licenseNumber,showAddress,legacyVivarealId,phones),medias,accountLink,link)),totalCount))&size=20&from={}&q=&developmentsSize=5&__vt=zpf:b&levels=CITY,UNIT_TYPE&ref=/venda/sp/santos/apartamento_residencial/&pointRadius="
        self.headers = {
            "Accept": "*/*",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0",
            "x-domain": "www.vivareal.com.br" 
        }
    
    @on_exception(expo, requests.exceptions.HTTPError, max_tries=4)
    def connect_to_endpoint(self, url: str) -> list:
        time.sleep(2)
        response = requests.request("GET", url=url, headers=self.headers)
        logger.info(f"Getting data from endpoint: {url}")
        logger.info(f"Status code: {response.status_code}")
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response.json()["search"]["result"]["listings"]
    
    def paginate(self, page=0):
        logger.info(f"Sending request Page {page}")
        full_url = self.url.format(page)
        time.sleep(2)
        data = self.connect_to_endpoint(full_url)
        page += len(data)
        yield data
        if page <= 100:
            yield from self.paginate(page)

    def run(self):
        yield from self.paginate()

if __name__ == "__main__":
    for pg in VivarealHook().run():
        for i in pg:
            print(json.dumps(i, indent=4))