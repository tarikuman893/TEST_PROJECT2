<?php

namespace App\Console\Commands\ImportMeasure;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportMeasureMp extends Command
{
    protected $signature   = 'import:measure-mp';
    protected $description = 'MP 計測 CSV を measure_mp へ取込む';

    public function handle(): int
    {
        $sourceDir  = storage_path('app/csvA8/Measure');
        $csvPattern = "$sourceDir/mp_measure.csv";

        // 日付文字列を null に変換
        $toDate = static function (?string $v): ?string {
            $v = trim($v ?? '');
            if ($v === '' || $v === '0000-00-00 00:00:00') {
                return null;
            }
            $ts = strtotime($v);
            return $ts === false ? null : date('Y-m-d H:i:s', $ts);
        };

        // 整数化
        $toInt = static fn (?string $v): int => (int) str_replace(',', '', $v ?? '0');

        foreach (glob($csvPattern) as $csv) {
            // SJIS→UTF-8（BOM 付き UTF-8 は auto で正しく変換）
            $tmp = tempnam(sys_get_temp_dir(), 'csv_') . '.csv';
            file_put_contents(
                $tmp,
                mb_convert_encoding(file_get_contents($csv), 'UTF-8', 'auto')
            );

            SimpleExcelReader::create($tmp)
                ->useDelimiter(',')
                ->noHeaderRow()
                ->getRows()
                ->reject(function (array $row) {
                    array_pop($row); // 余分列を削除
                    if (empty($row)) {
                        return true;
                    }
                    if (strpos($row[0] ?? '', 'タイプ') !== false) {
                        return true;
                    }
                    // 除外条件
                    if (($row[3] ?? '') === '2025/07/18 17:00:52') return true;
                    if (($row[3] ?? '') === '2025/07/18 10:24:42') return true;
                    if (($row[3] ?? '') === '2025/07/17 17:31:26') return true;
                    if (($row[4] ?? '') === '2025-07-16 10:23:34') return true;
                    if (($row[4] ?? '') === '2025-07-15 18:46:10') return true;
                    if (($row[4] ?? '') === '2025-07-15 08:43:48') return true;
                    if (($row[4] ?? '') === '2025-07-15 08:03:19') return true;
                    if (($row[4] ?? '') === '2025-07-14 17:51:03') return true;
                    if (($row[4] ?? '') === '2025-07-13 10:24:20') return true;
                    if (($row[4] ?? '') === '2025-07-12 23:53:32') return true;
                    if (($row[4] ?? '') === '2025-07-11 15:03:20') return true;
                    if (($row[4] ?? '') === '2025-07-09 22:22:37') return true;
                    if (($row[4] ?? '') === '2025-07-09 08:14:30') return true;
                    if (($row[4] ?? '') === '2025-07-07 11:44:08') return true;
                    if (($row[4] ?? '') === '2025-07-05 20:54:31') return true;
                    if (($row[4] ?? '') === '2025-07-05 16:32:07') return true;
                    return false;
                })
                ->each(function (array $row) use ($toDate, $toInt) {
                    array_pop($row);

                    // MeasureID／TCLICKID／gclid／utm_content を正しく分割
                    $param = $row[8] ?? '';
                    if (strpos($param, '_YCLID_') !== false) {
                        $parts = explode('_YCLID_', $param, 2);
                    } else {
                        $parts = explode('_GCLID_', $param, 2);
                    }
                    $measureId  = null;
                    $tclickId   = null;
                    $gclid      = null;
                    $utmContent = null;
                    if (!empty($parts[0]) && strpos($parts[0], '_TCLICK_') !== false) {
                        [$measureId, $tclickId] = explode('_TCLICK_', $parts[0], 2);
                    } else {
                        $measureId = $parts[0] ?? null;
                    }
                    if (!empty($parts[1]) && strpos($parts[1], '_UTMC_') !== false) {
                        [$gclid, $utmContent] = explode('_UTMC_', $parts[1], 2);
                    } else {
                        $gclid = $parts[1] ?? null;
                    }

                    DB::table('measure_mp')->insertOrIgnore([
                        'archive_type'        => $row[0]               ?? null,
                        'pg_name'             => $row[1]               ?? null,
                        'reward'              => $toInt($row[2]       ?? null),
                        'click_time'          => $toDate($row[3]      ?? null),
                        'occur_time'          => $toDate($row[4]      ?? null),
                        'fix_time_new_column' => $toDate($row[5]      ?? null),
                        'fix_time'            => $toDate($row[6]      ?? null),
                        'statement'           => $row[7]               ?? null,
                        'user_id'             => $measureId,
                        'gclid'               => $gclid,
                        'utm_content'         => $utmContent,
                        'device'              => $row[9]               ?? null,
                        'referer'             => $row[10]              ?? null,
                        'keyword'             => $row[11]              ?? null,
                        'site_name'           => $row[12]              ?? null,
                        'tclick'              => $tclickId,
                    ]);
                });

            unlink($tmp);
        }

        $this->info('MP 計測データ取込完了');
        return Command::SUCCESS;
    }
}
