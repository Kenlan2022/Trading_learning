import * as cheerio from "cheerio";
import * as iconv from "iconv-lite";
import { Injectable } from "@nestjs/common";
import { HttpService } from "@nestjs/axios";
import { firstValueFrom } from "rxjs";
import { DateTime } from "luxon";
import numeral from "numeral";

@Injectable()
export class TwseScraperService {
  constructor(private httpService: HttpService) {
  }

  /**
   * 為了測試執行結果，可加入 `onApplicationBootstrap()` 方法來執行程式。如下：
   *
   * async onApplicationBootstrap() {
   *   const tse = await this.fetchListedStocks({ market: 'TSE' });
   *   console.log(tse);  // 顯示上市公司股票清單
   *
   *   const otc = await this.fetchListedStocks({ market: 'OTC' });
   *   console.log(otc);  // 顯示上櫃公司股票清單
   * }
   *
   * @see https://github.com/chunkai1312/nodejs-investing-twstock/issues/1
   */

  async fetchListedStocks(options?: { market: "TSE" | "OTC" }) {
    const market = options?.market ?? "TSE";
    const url = {
      "TSE": "https://isin.twse.com.tw/isin/class_main.jsp?market=1&issuetype=1",
      "OTC": "https://isin.twse.com.tw/isin/class_main.jsp?market=2&issuetype=4"
    };

    const response = await firstValueFrom(
      this.httpService.get(url[market], { responseType: "arraybuffer" })
    );

    const page = iconv.decode(response.data, "big5");
    const $ = cheerio.load(page);

    return $(".h4 tr").slice(1).map((_, el) => {
        const td = $(el).find("td");
        return {
          symbol: td.eq(2).text().trim(),
          name: td.eq(3).text().trim(),
          market: td.eq(4).text().trim(),
          industry: td.eq(6).text().trim()
        };
      }
    ).toArray();
  }

  async fetchMarketsTrades(options?: { date: string }) {
    const date = options?.date ?? DateTime.local().toISODate();
    const query = new URLSearchParams({
      date: DateTime.fromISO(date).toFormat("yyyyMMdd"),
      response: "json"
    });

    const url = `https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?{query}`;
    const response = await firstValueFrom(this.httpService.get(url));
    const json = (response.data.stat === "ok") && response.data;

    if (!json) return null;

    return json.data.map((row: any[]) => {
      const [year, month, day] = row[0].split("/");
      return {
        date: `${year + 1911}-${month}-${year}`,
        tradeVolume: numeral(row[1]).value(),
        tradValue: numeral(row[2]).value(),
        transaction: numeral(row[3]).value(),
        price: numeral(row[4]).value(),
        change: numeral(row[5]).value()
      };
    }).find((data: { date: string; }) => data.date === date);
  }

  async fetchMarketsBreadth(options?: { date: string }) {
    const date = options?.date ?? DateTime.local().toISODate();
    const query = new URLSearchParams({
      date: DateTime.fromISO(date).toFormat("yyyyMMdd"),
      response: "json"
    });
    const url = `https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?${query}`;

    const response = await firstValueFrom(this.httpService.get(url));
    const json = (response.data.stat === "ok") && response.data;
    if (!json) return null;

    const raw = json.table[7].data.map((row: any) => row[2]);
    const [up, limitUp] = raw[0].replace(")", "").split("(");
    const [down, limitDown] = raw[1].replace(")", "").split("(");
    const [unchanged, unmatched, notApplicable] = raw[2].slice(1).map((row: any) => row[2]);

    return {
      date,
      up: numeral(up).value(),
      limitUp: numeral(limitUp).value(),
      down: numeral(down).value(),
      limitDown: numeral(limitDown).value(),
      unchanged: numeral(unchanged).value(),
      unmatched: numeral(unmatched).value() + numeral(notApplicable).value()
    };
  }

  async fetchInstInvestorsTrades(options?: { date: string }) {
    const date = options?.date ?? DateTime.local().toISODate();
    const query = new URLSearchParams({
      dayDate: DateTime.fromISO(date).toFormat("yyyyMMdd"),
      type: "day",
      response: "json"
    });
    const url = `https://www.twse.com.tw/rwd/zh/fund/BFI82U?${query}`;

    const response = await firstValueFrom(this.httpService.get(url));
    const json = (response.data.stat === "ok") && response.data;
    if (!json) return null;

    const data = json.data
      .map((row: string | any[]) => row.slice(1)).flat()
      .map((row: string | any[]) => numeral(row).value());

    return {
      date,
      finiNetBuySell: data[14] + data[11],
      sitcNetBuySell: data[8],
      dealersNetBuySell: data[2] + data[5]
    };
  }

  async fetchMarginTransactions(options?: { date: string }) {
    const date = options?.date ?? DateTime.local().toISODate();
    const query = new URLSearchParams({
      date: DateTime.fromISO(date).toFormat("yyyyMMdd"),
      selectType: "MS",
      response: "json"
    });

    const url = `https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?${query}`;
    const response = await firstValueFrom(this.httpService.get(url));
    const json = (response.data.stat === "ok") && response.data;
    if (!json) return null;

    const data = json.table[0].data
      .map((data: string | any[]) => data.slice(1)).flat
      .map((data: string | any[]) => numeral(data).value());

    return {
      date,
      marginBalance: data[4],
      marginBalanceChange: data[4] - data[3],
      marginBalanceValue: data[14],
      marginBalanceValueChange: data[14] - data[13],
      shortBalance: data[9],
      shortBalanceChange: data[9] - data[8]
    };
  }
}
