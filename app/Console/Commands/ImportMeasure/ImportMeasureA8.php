<?php

namespace App\Console\Commands\ImportMeasure;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportMeasureA8 extends Command
{
    protected $signature   = 'import:measure-a8';
    protected $description = 'A8 など計測 CSV を measure_a8new へ取込む';

    public function handle(): int
    {
        $sourceDir  = storage_path('app/csvA8/Measure');
        $headers    = ['プログラムID'];
        $csvPattern = "$sourceDir/*.csv";

        $toDate = static function (?string $v): ?string {
            $v = trim($v ?? '');
            if ($v === '' || $v === '-') {
                return null;
            }
            $ts = strtotime($v);
            return $ts === false ? null : date('Y-m-d H:i:s', $ts);
        };

        $toInt = static fn (?string $v): int => (int) str_replace(',', '', $v ?? '0');

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
                ->reject(function (array $row) use ($headers) {
                    return empty($row) || in_array($row[0] ?? '', $headers, true);
                })
                ->each(function (array $row) use ($toDate, $toInt) {
                    // 計測 ID・TCLICK ID
                    $paramSource = trim((($row[3] ?? '') !== '') ? ($row[4] ?? '') : ($row[2] ?? ''));
                    if (strpos($paramSource, '_TCLICK_') !== false) {
                        [$measureId, $tclickId] = explode('_TCLICK_', $paramSource, 2);
                    } else {
                        $measureId = $paramSource;
                        $tclickId  = null;
                    }

                    // gclid / utm_content
                    $gclid      = null;
                    $utmContent = null;
                    $rawGclid   = $row[3] ?? '';
                    if (strpos($rawGclid, '_UTMC_') !== false) {
                        [$gclid, $utmContent] = explode('_UTMC_', $rawGclid, 2);
                    } else {
                        $gclid      = ($rawGclid !== '') ? $rawGclid : null;
                        $utmContent = null;
                    }

                    DB::table('measure_a8new')->insertOrIgnore([
                        'pg_id'        => $row[0]  ?? null,
                        'pg_name'      => $row[1]  ?? null,
                        'point_id1'    => $row[2]  ?? null,
                        'point_id2'    => $row[3]  ?? null,
                        'point_id3'    => $row[4]  ?? null,
                        'point_id4'    => $row[5]  ?? null,
                        'point_id5'    => $row[6]  ?? null,
                        'statement'    => $row[7]  ?? null,
                        'click_time'   => $toDate($row[8]  ?? null),
                        'occur_time'   => $toDate($row[9]  ?? null),
                        'fix_time'     => $toDate($row[10] ?? null),
                        'reward'       => $toInt($row[11] ?? null),
                        'fix_reward'   => $toInt($row[12] ?? null),
                        'sales'        => $toInt($row[13] ?? null),
                        'order_id'     => $row[14] ?? null,
                        'program_id'   => $row[15] ?? null,
                        'program_type' => $row[16] ?? null,
                        'device'       => $row[17] ?? null,
                        'referer'      => $row[18] ?? null,
                        'tclick'       => $tclickId,
                        'measureid_id' => $measureId,
                        'gclid'        => $gclid,
                        'utm_content'  => $utmContent,
                    ]);
                });

            unlink($tmp);
        }

        $this->info('計測データ取込完了');
        return Command::SUCCESS;
    }
}
