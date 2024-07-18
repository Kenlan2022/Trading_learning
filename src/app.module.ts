import { Module } from '@nestjs/common';
import { HttpModule } from "@nestjs/axios";
import { ScraperModule } from './scraper/scraper.module';


@Module({
    imports :[HttpModule, ScraperModule],
})
export class AppModule {}
