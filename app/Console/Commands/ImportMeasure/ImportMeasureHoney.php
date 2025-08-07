<?php

namespace App\Console\Commands\ImportMeasure;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportMeasureHoney extends Command
{
    /** コマンド名 */
    protected $signature   = 'import:measure-honey';

    /** 説明 */
    protected $description = 'Honey 計測 CSV を measure_honey へ取込む';

    /* ---------- 共通ヘルパ ---------- */
    private static function toDate(?string $v): ?string
    {
        $v = trim($v ?? '');
        if ($v === '' || $v === '0000-00-00 00:00:00') {
            return null;
        }
        $ts = @strtotime(str_replace('/', '-', $v));
        return $ts === false ? null : date('Y-m-d H:i:s', $ts);
    }

    private static function toInt(?string $v): int
    {
        if ($v === null || $v === '') return 0;
        return (int) floor((float) str_replace(',', '', $v));
    }

    /* ---------- 分割ユーティリティ ---------- */
    private static function extractMeasureId(string $field): string
    {
        if (($pos = strpos($field, '_TCLICK_')) !== false) {
            return substr($field, 0, $pos);
        }
        foreach (['_YCLID_', '_GCLID_', '_CLID_'] as $tag) {
            if (($pos = strpos($field, $tag)) !== false) {
                return substr($field, 0, $pos);
            }
        }
        return $field;
    }

    private static function extractTclickId(string $field): string
    {
        if (strpos($field, '_TCLICK_') === false) return '';
        [, $rest] = explode('_TCLICK_', $field, 2);
        foreach (['_YCLID_', '_GCLID_', '_CLID_'] as $tag) {
            $rest = explode($tag, $rest, 2)[0];
        }
        return $rest;
    }

    private static function extractClid(string $field): string
    {
        foreach (['_YCLID_', '_GCLID_', '_CLID_'] as $tag) {
            if (strpos($field, $tag) !== false) {
                [, $right] = explode($tag, $field, 2);
                return (strpos($right, '_UTMC_') !== false)
                    ? explode('_UTMC_', $right, 2)[0]
                    : $right;
            }
        }
        return '';
    }

    private static function extractUtmContent(string $field): string
    {
        foreach (['_YCLID_', '_GCLID_', '_CLID_'] as $tag) {
            if (strpos($field, $tag) !== false && strpos($field, '_UTMC_') !== false) {
                [, $right] = explode($tag, $field, 2);
                [, $utm]  = explode('_UTMC_', $right, 2);
                return $utm;
            }
        }
        return '';
    }

    /* ---------- Honey 固有ステップ ---------- */
    private static function honey_measure_01_fixMissingTracking(array &$l): void
    {
        $replacements = [
            'R58125452' => 'gs_cardloan-1_acom_honeycomb_TCLICK__CLID_Cj0KCQjws-S-BhD2ARIsALssG0Z2E7pdWg1pTvoGaPMkSsyya2QOi8CIJdV6N8vnX5njt2sBO-Iiv44aAkkXEALw_wcB_UTMC_174499321617',
            'R58125816' => 'gs_cardloan-1_acom_honeycomb_TCLICK__CLID_Cj0KCQjws-S-BhD2ARIsALssG0Z2F9QYAToal0zQptOJOlh3b-BKZ6k1NQrqH1wuH3VUNJBGrNbKY6UaAl0VEALw_wcB_UTMC_174499321617',
            'R58267994' => 'gs_cardloan-1_acom_honeycomb_TCLICK__CLID_Cj0KCQjwhYS_BhD2ARIsAJTMMQY26Ny-cyaLwHmyUQ-0u3zZT3feWV_obANn5kW1FqhWFj9nx4JAcQoaAlgxEALw_wcB_UTMC_174499321617',
            'R58267398' => 'gs_cardloan-1_promise_honeycomb_TCLICK__CLID_Cj0KCQjwhYS_BhD2ARIsAJTMMQayzQsM5VOAVfNRDgKHzWyGbUR6NFgloLd8kE8oTD4YtY7Nq7anfzUaAj2yEALw_wcB_UTMC_174499321617',
        ];
        if (isset($replacements[$l[0] ?? ''])) {
            $l[14] = $replacements[$l[0]];
        }
    }

    private static function honey_measure_02_excludeRows(array &$l): bool
    {
        if (($l[0] ?? '') === '' || ($l[0] ?? '') === "\x1A") return false;
        if (($l[1] ?? '') === '2021-07-30 16:15:35')        return false;
        return true;
    }

    /* ---------- 固定報酬 ---------- */
    private static function fixedReward(string $measureId, ?string $occurDate): int
    {
        if ($measureId === '') return 0;
        $dateThreshold = '2025-05-20';
        $occur = substr($occurDate ?? '', 0, 10);

        $before = [
            'acom'    => 12000,
            'promise' => 14000,
            'aiful'   => 9000,
            'mobit'   => 14000,
        ];
        $after = [
            'acom'    => 75000,
            'promise' => 14000,
            'aiful'   => 9000,
            'mobit'   => 14000,
        ];
        $table = ($occur !== '' && $occur < $dateThreshold) ? $before : $after;

        foreach ($table as $key => $value) {
            if (strpos($measureId, $key) !== false) return $value;
        }
        return 0;
    }

    /* ---------- 行フォーマット ---------- */
    private static function formatRow(array $line)
    {
        $steps = array_filter(
            get_class_methods(self::class),
            fn($m) => str_starts_with($m, 'honey_measure_')
        );
        natsort($steps);

        foreach ($steps as $step) {
            $res = self::{$step}($line);
            if ($res === false) return false;
        }

        $tracking  = $line[14] ?? '';
        $measureId = self::extractMeasureId($tracking);
        $tclickId  = self::extractTclickId($tracking);
        $gclid     = self::extractClid($tracking);
        $utm       = self::extractUtmContent($tracking);

        if ($measureId === '') {
            switch ($line[4] ?? '') {
                case '株式会社オプト(アコム)':
                    $measureId = 'gs_cardloan-1_acom_honeycomb';
                    break;
                case 'アイフル株式会社':
                    $measureId = 'gs_cardloan-1_aiful_honeycomb';
                    break;
                case 'SMBCモビット':
                    $measureId = 'gs_cardloan-1_mobit_honeycomb';
                    break;
                case 'プロミスのキャッシングプロモーション':
                    $measureId = 'gs_cardloan-1_promise_honeycomb';
                    break;
            }
            if ($gclid === '') $gclid = $tracking;
        }

        $reward = self::fixedReward($measureId, self::toDate($line[2] ?? null));

        return [
            'order_id'       => $line[0]  ?? null,
            'click_time'     => self::toDate($line[1] ?? null),
            'occur_time'     => self::toDate($line[2] ?? null),
            'fix_time'       => self::toDate($line[3] ?? null),
            'pg_name'        => $line[4]  ?? null,
            'pg_flg'         => $line[5]  ?? null,
            'campaign'       => $line[6]  ?? null,
            'lpid'           => $line[7]  ?? null,
            'lp'             => $line[8]  ?? null,
            'lpurl'          => $line[9]  ?? null,
            'device'         => $line[10] ?? null,
            'os'             => $line[11] ?? null,
            'statement'      => $line[12] ?? null,
            'referer'        => $line[13] ?? null,
            'tracking_param' => $tracking ?: null,
            'reward'         => $reward,
            'measure_id'     => $measureId ?: null,
            'gclid'          => $gclid     ?: null,
            'utm_content'    => $utm       ?: null,
            'tclick'         => $tclickId  ?: null,
        ];
    }

    /* ---------- メイン ---------- */
    public function handle(): int
    {
        $sourceDir  = storage_path('app/csv/Measure');
        $csvPattern = "$sourceDir/*honey*.csv";
        $headers    = ['成果ID'];

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
                ->reject(fn($row) => empty($row) || in_array($row[0] ?? '', $headers, true))
                ->each(function (array $row) {
                    $row = self::formatRow($row);
                    if ($row === false) return;

                    DB::table('measure_honey')->insert($row);
                });

            unlink($tmp);
        }

        $this->info('Honey 計測データ取込完了');
        return Command::SUCCESS;
    }
}
