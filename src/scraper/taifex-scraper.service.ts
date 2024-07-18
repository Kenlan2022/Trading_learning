import { Injectable } from "@nestjs/common";
import { HttpService } from "@nestjs/axios";
import { DateTime } from "luxon";
import { firstValueFrom } from "rxjs";
import csvtojson from "csvtojson";
import iconv from "iconv-lite";
import numeral from "numeral";

@Injectable()
export class TaifexScraperService {
  constructor(private httpService: HttpService) {
  }

  async fetchInstInvestorsTxfTrades(options: { date: string }) {
    const date = options?.date ?? DateTime.local().toISODate();
    const queryDate = DateTime.fromISO(date).toFormat("yyyy/MM/dd");
    const firstDate = DateTime.fromISO(date).minus({ years: 3 }).toFormat("yyyy/MM/dd HH:mm");
    const lastDate = DateTime.fromISO(date).toFormat("yyyy/MM/dd HH:mm");

    const form = new URLSearchParams({
      queryStartDate: queryDate,
      queryEndDate: queryDate,
      commodityId: "TXF",
      firstDate: firstDate,
      lastDate: lastDate
    });
    const url = "https://www.taifex.com.tw/cht/3/futCOntractsDateDown";
    const response = await firstValueFrom(this.httpService.post(url, form, {
      responseType: "arraybuffer"
    }));

    const json = await csvtojson({ noheader: true, output: "csv" }).fromString(iconv.decode(response.data, "big5"));
    const [fields, dealers, sitc, fini] = json;
    if (fields[0] !== "日期") return null;

    return {
      date,
      finiTxfNetOi: numeral(fini[13]).value(),
      sitcTxNetOi: numeral(sitc[13]).value(),
      dealersTxfNetOi: numeral(dealers[13]).value()
    };
  }

  async fetchInstInvestorsTxoTrades(options?:{date: string}){
    const date = options?.date ?? DateTime.local().toISODate();
    const queryDate = DateTime.fromISO(date).toFormat('yyyy/MM/dd');
    const firstDate = DateTime.fromISO(date).minus({ years: 3 }).toFormat("yyyy/MM/dd HH:mm");
    const lastDate = DateTime.fromISO(date).toFormat("yyyy/MM/dd HH:mm");

    const form = new URLSearchParams({
      queryStartDate: queryDate,
      queryEndDate: queryDate,
      commodityId: "TXO",
      firstDate: firstDate,
      lastDate: lastDate
    })

    const url = 'https://www.taifex.com.tw/cht/3/callsAndPutsDateDown';

    const response = await firstValueFrom(this.httpService.post(url, form, {
      responseType: 'arraybuffer'
    }));
    const json = await csvtojson({noheader: true, output: 'csv'}).fromString(iconv.decode(response.data, 'big5'));
    const [fields, dealersCalls, sitcCalls, finiCalls, dealerPuts, sitcPuts, finiPuts] = json;
    if(fields[0] !== '日期') return null;
    return {
      date,
      finiTxoCallsNetOi: numeral(finiCalls[14]).value(),
      finiTxoCallsNetOiValue: numeral(finiCalls[15]).value(),
      sitcTxoCallsNetOi: numeral(sitcCalls[14]).value(),
      sitcTxoCallsNeOiValue: numeral(sitcCalls[15]).value(),
      dealersTxoCallsNetOi: numeral(dealersCalls[14]).value(),
      dealersTxoCallsNetOiValue: numeral(dealersCalls[15]).value(),
      finiTxoPutsNetOi: numeral(finiPuts[14]).value(),
      finiTxoPutsNetOiValue: numeral(finiPuts[15]).value(),
      sitcTxoPutsNetOi: numeral(sitcPuts[14]).value(),
      sitcTxoPutsNeOiValue: numeral(sitcPuts[15]).value(),
      dealersTxoPutsNetOi: numeral(dealerPuts[14]).value(),
      dealersTxoPutsNetOiValue: numeral(dealerPuts[15]).value()
    }
  }

  private async fetchMxfMarketOi(options?: {date: string}){
    const date = options?.date ?? DateTime.local().toISODate();
    const queryDate = DateTime.fromISO(date).toFormat('yyyy/MM/dd');
    const form = new URLSearchParams({
      down_type: '1',
      queryStartDate: queryDate,
      queryEndDate: queryDate,
      commodity_id: 'MTX',
    })
    const url = 'https://www.taifex.com.tw/cht/3/futDataDown';

    const response = await firstValueFrom(this.httpService.post(url, form, {
      responseType: 'arraybuffer'
    }));

    const json = await csvtojson({noheader: true, output: 'csv'}).fromString(
      iconv.decode(response.data, 'big5'));
    const [fields, ...rows] = json;
    if(fields[0] !== '交易日期') return null;

    const mxfMarketOi = rows
      .filter(row => row[17] === '一般' && row[18])
      .reduce((oi, row) => oi + numeral(row[11]).value(), 0);

    return {date, mxfMarketOi}
  }
}
