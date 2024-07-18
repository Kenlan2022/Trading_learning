import { Module } from '@nestjs/common';
import { TwseScraperService } from './twse-scraper.service';
import { HttpModule } from "@nestjs/axios";
import { TpexScraperService } from './tpex-scraper.service';
import { TaifexScraperService } from './taifex-scraper.service';

@Module({
  imports: [HttpModule],
  providers: [TwseScraperService, TpexScraperService, TaifexScraperService]
})
export class ScraperModule {}
