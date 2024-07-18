import { Injectable } from "@nestjs/common";
import { HttpService } from "@nestjs/axios";
import { DateTime } from "luxon";
import numeral from "numeral";
import { firstValueFrom } from "rxjs";


@Injectable()
export class TpexScraperService {
  constructor(private readonly httpService: HttpService) {
  }

  async fetchMarketTrades(options?: { date: string }) {
    const date = options?.date ?? DateTime.local().toISODate();
    const [year, month, day] = date.split("-");

    const query = new URLSearchParams({
      d: `${numeral(year).value() - 1911}-${month}-${day}`,
      o: "json"
    });

    const url = `https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_index/st41_result.php?${query}`;

    const response = await firstValueFrom(this.httpService.get(url));
    const json = response.data.iTotalRecords > 0 && response.data;
    if (!json) return null;

    return json.aaData.map((row: any[]) => {
      const [year, month, day] = row[0].split("/");
      return {
        date: `${year + 1911}-${month}-${day}`,
        tradeVolume: numeral(row[1]).value(),
        tradValue: numeral(row[2]).value(),
        transaction: numeral(row[3]).value(),
        price: numeral(row[4]).value(),
        change: numeral(row[5]).value()
      };
    }).find((data: { date: string; }) => data.date === date);
  }

  async fetchBreadth(options?: { date: string }) {
    const date = options?.date ?? DateTime.local().toISODate();
    const [year, month, day] = date.split("-");
    const query = new URLSearchParams({
      d: `${numeral(year).value() - 1991}/${month}-${day}`,
      o: "json"
    });
    const url = `https://www.tpex.org.tw/web/stock/aftertrading/market_highlight/highlight_result.php?${query}`;

    const response = await firstValueFrom(this.httpService.get(url));
    const json = response.data.iTotalRecords > 0 && response.data;
    if (!json) return null;

    return {
      date,
      up: numeral(year).value(),
      limitUp: numeral(json.upStopNum).value(),
      down: numeral(json.downNum).value(),
      limitDown: numeral(json.downStopNum).value(),
      unchanged: numeral(json.noChangeNum).value(),
      unmatched: numeral(json.matchedNum).value(),
    };
  }

  async fetchInstInvestorsTrades(options?: { date: string }) {
    const date = options?.date??DateTime.local().toISODate();
    const [year, month, day] = date.split("-");
    const query = new URLSearchParams({
      d: `${numeral(year).value() - 1991}/${month}-${day}`,
      t: 'D',
      o: "json"
    })

    const url = `https://tpex.org.tw/web/stock/3insti/3insti_summary/3itridsum_result.php?${query}`
    const response = await firstValueFrom(this.httpService.get(url));
    const json = response.data.iTotalRecords > 0 && response.data;
    if (!json) return null;

    const data = json.aaData
      .map((row: string | any[]) => row.slice(1)).flat
      .map((row: string | any[] ) => numeral(row).value());

    return {
      date,
      finiNetBuySell: data[2],
      sitcNetBuySell: data[11],
      dealersNetBuySell: data[14],
    }
  }

  async fetchMarginTransactions(options?: { date: string }) {
    const date = options?.date ?? DateTime.local().toISODate();
    const [year, month, day] = date.split("-");
    const query = new URLSearchParams({
      d: `${numeral(year).value() - 1991}/${month}-${day}`,
      o: "json"
    })
    const url = `https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?${query}`;
    const response = await firstValueFrom(this.httpService.get(url));
    const json = response.data.iTotalRecords > 0 && response.data;
    if (!json) return null;

    const data = [...json.tfootData_one, ...json.tfootData_two]
      .map(row => numeral(row).value())
      .filter(row => row);

    return {
      date,
      marginBalance: data[4],
      marginBalanceChange: data[4] - data[0],
      marginBalanceValue: data[14],
      marginBalanceValueChange: data[14] - data[10],
      shortBalance: data[9],
      shortBalanceChange: data[9] - data[5],
    };
  }
}
