<?php

namespace App\Console\Commands\ImportMeasure;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportMeasureVc extends Command
{
    protected $signature   = 'import:measure-vc';
    protected $description = 'Vc 計測 CSV を measure_vc へ取込む';

    /* ---------- 共通ヘルパ ---------- */
    private static function toDate(?string $v): ?string
    {
        $v = trim($v ?? '');
        if ($v === '' || $v === '−') {
            return null;
        }
        $ts = @strtotime(str_replace('/', '-', preg_replace('/（.*）/', '', $v)));
        return $ts === false ? null : date('Y-m-d H:i:s', $ts);
    }

    private static function toInt(?string $v): int
    {
        if ($v === null || $v === '' || $v === '−') {
            return 0;
        }
        return (int) floor((float) str_replace(',', '', $v));
    }

    /* ---------- Vc 固有ステップ ---------- */
    private static function vc_measure_01_excludeRows(array &$l): bool
    {
        if (empty($l[3] ?? '')) {
            return false;
        }
        return true;
    }

    /* ---------- 行フォーマット ---------- */
    private static function formatRow(array $line)
    {
        $steps = array_filter(
            get_class_methods(self::class),
            fn($m) => str_starts_with($m, 'vc_measure_')
        );
        natsort($steps);
        foreach ($steps as $step) {
            $res = self::{$step}($line);
            if ($res === false) {
                return false;
            }
        }

        $program    = $line[5]  ?? null;
        $siteName   = $line[16] ?? null;
        $pgName     = $program !== null
            ? $program . ($siteName !== null ? $siteName : '')
            : null;

        return [
            'click_time'     => self::toDate($line[0]  ?? null),
            'occur_time'     => self::toDate($line[1]  ?? null),
            'fix_time'       => self::toDate($line[2]  ?? null),
            'order_id'       => $line[3]             ?? null,
            'ad_owner'       => $line[4]             ?? null,
            'pg_name'        => $pgName,
            'keyword'        => $line[6]             ?? null,
            'sales_count'    => self::toInt($line[7]  ?? null),
            'sales'          => (float) str_replace(',', '', $line[8]  ?? '0'),
            'reward'         => (float) str_replace(',', '', $line[9]  ?? '0'),
            'tax'            => (float) str_replace(',', '', $line[10] ?? '0'),
            'reward_tax'     => (float) str_replace(',', '', $line[11] ?? '0'),
            'statement'      => $line[12]            ?? null,
            'approval_period'=> $line[13]            ?? null,
            'referer'        => $line[14]            ?? null,
            'device'         => $line[15]            ?? null,
            'site_name'      => $siteName
        ];
    }

    /* ---------- メイン ---------- */
    public function handle(): int
    {
        $sourceDir = storage_path('app/csv/Measure');
        $pattern   = "$sourceDir/*vc*.csv";
        $headers   = ['クリック日'];

        foreach (glob($pattern) as $csv) {
            $tmp = tempnam(sys_get_temp_dir(), 'csv_') . '.csv';
            file_put_contents(
                $tmp,
                mb_convert_encoding(file_get_contents($csv), 'UTF-8', 'SJIS-win,CP932,UTF-8')
            );

            SimpleExcelReader::create($tmp)
                ->useDelimiter(',')
                ->noHeaderRow()
                ->getRows()
                ->reject(fn($row) => empty($row) || in_array($row[0] ?? '', $headers, true))
                ->each(function (array $row) {
                    $row = self::formatRow($row);
                    if ($row === false) {
                        return;
                    }
                    DB::table('measure_vc')->insert($row);
                });

            unlink($tmp);
        }

        $this->info('Vc 計測データ取込完了');
        return Command::SUCCESS;
    }
}
