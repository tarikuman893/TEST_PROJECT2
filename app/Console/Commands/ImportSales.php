<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportSales extends Command
{
    protected $signature   = 'import:sales';
    protected $description = 'A8 など売上 CSV を csv_a8new へ取込む';

    public function handle(): int
    {
        /* ---------- 設定 ---------- */
        $sourceDir   = storage_path('app/csvSales');
        $headers     = ['プログラムID'];
        $csvPattern  = "$sourceDir/*.csv";

        /* ---------- ヘルパ ---------- */
        $toDate = static function (?string $v): ?string {
            $v = trim($v ?? '');
            if ($v === '' || $v === '-') {
                return null;
            }
            $ts = strtotime($v);
            return $ts === false ? null : date('Y-m-d H:i:s', $ts);
        };
        $toInt = static fn (?string $v): int => (int) str_replace(',', '', $v ?? '0');

        /* ---------- 取込 ---------- */
        foreach (glob($csvPattern) as $csv) {
            $tmp = tempnam(sys_get_temp_dir(), 'csv_') . '.csv';
            file_put_contents(
                $tmp,
                mb_convert_encoding(file_get_contents($csv), 'UTF-8', 'SJIS-win,CP932')
            );

            SimpleExcelReader::create($tmp)
                ->useDelimiter(',')
                ->noHeaderRow()
                ->getRows()
                ->reject(function ($row) use ($headers, $toDate) {
                    if (empty($row)) {
                        return true;
                    }
                    if (in_array($row[0] ?? '', $headers, true)) {
                        return true;
                    }
                    return $toDate($row[4] ?? '') === null && ($row[4] ?? '') !== '';
                })
                ->each(function (array $row) use ($toDate, $toInt) {
                    DB::table('csv_a8new')->insert([
                        'pg_id'      => $row[0] ?? null,
                        'pg_name'    => $row[1] ?? null,
                        'statement'  => $row[2] ?? null,
                        'type'       => $row[3] ?? null,
                        'click_time' => $toDate($row[4] ?? null),
                        'occur_time' => $toDate($row[5] ?? null),
                        'fix_time'   => $toDate($row[6] ?? null),
                        'reward'     => $toInt($row[7]  ?? null),
                        'fix_reward' => $toInt($row[8]  ?? null),
                        'sales'      => $toInt($row[9]  ?? null),
                        'order_id'   => $row[10] ?? null,
                        'ad_id'      => $row[11] ?? null,
                        'device'     => $row[12] ?? null,
                        'referer'    => $row[13] ?? null,
                    ]);
                });

            unlink($tmp);
        }

        $this->info('売上データ取込完了');
        return Command::SUCCESS;
    }
}
