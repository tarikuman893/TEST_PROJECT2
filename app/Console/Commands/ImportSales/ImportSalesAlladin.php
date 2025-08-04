<?php

namespace App\Console\Commands\ImportSales;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportSalesAlladin extends Command
{
    protected $signature   = 'import:sales-alladin';
    protected $description = 'Alladin 売上 CSV を csv_alladin へ取込む';

    private static function toDate(?string $v): ?string
    {
        $v = trim($v ?? '');
        if ($v === '' || $v === '0000-00-00 00:00:00') return null;
        $ts = @strtotime(str_replace('/', '-', $v));
        return $ts === false ? null : date('Y-m-d H:i:s', $ts);
    }

    private static function toInt(?string $v): int
    {
        return (int) str_replace(',', '', $v ?? '0');
    }

    public function handle(): int
    {
        $sourceDir  = storage_path('app/csv/Sales');
        $csvPattern = "$sourceDir/*alladin*.csv";
        $headers    = ['成果識別子'];

        foreach (glob($csvPattern) as $csv) {
            $tmp = tempnam(sys_get_temp_dir(), 'csv_') . '.csv';
            file_put_contents(
                $tmp,
                mb_convert_encoding(file_get_contents($csv), 'UTF-8', 'SJIS-win,CP932,UTF-8')
            );

            SimpleExcelReader::create($tmp)
                ->useDelimiter(',')
                ->noHeaderRow()
                ->getRows()
                ->reject(fn ($row) => empty($row) || in_array($row[0] ?? '', $headers, true))
                ->each(function (array $row) {
                    $measureId = (isset($row[1]) && strpos($row[1], '_TCLICK_') !== false)
                        ? explode('_TCLICK_', $row[1])[0]
                        : ($row[1] ?? null);

                    DB::table('csv_alladin')->insert([
                        'order_id'      => $row[0]  ?? null,
                        'measure_id'    => $measureId,
                        'click_time'    => self::toDate($row[2]  ?? null),
                        'occur_time'    => self::toDate($row[3]  ?? null),
                        'fix_time'      => self::toDate($row[4]  ?? null),
                        'media_id'      => $row[5]  ?? null,
                        'media_name'    => $row[6]  ?? null,
                        'reward'        => self::toInt($row[7]  ?? null),
                        'transaction'   => self::toInt($row[8]  ?? null),
                        'reward_rate'   => $row[9]  ?? null,
                        'statement'     => $row[10] ?? null,
                        'device'        => $row[11] ?? null,
                        'referer'       => $row[12] ?? null,
                        'gclid'         => $row[13] ?? null,
                    ]);
                });

            unlink($tmp);
        }

        $this->info('Alladin 売上データ取込完了');
        return Command::SUCCESS;
    }
}
