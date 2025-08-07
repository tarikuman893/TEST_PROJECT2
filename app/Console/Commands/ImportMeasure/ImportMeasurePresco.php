<?php

namespace App\Console\Commands\ImportMeasure;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportMeasurePresco extends Command
{
    /** コマンド名 */
    protected $signature   = 'import:measure-presco';

    /** 説明 */
    protected $description = 'Presco 計測 CSV を measure_presco へ取込む';

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

    /* ---------- 分割ユーティリティ（固定） ---------- */
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

    /* ---------- Presco 固有ステップ ---------- */

    /** 空行除外 */
    private static function presco_measure_01_excludeEmptyRow(array &$l): bool
    {
        return ($l[0] ?? '') !== '';
    }

    /** 2024-05-05 ふくちゃん是正 */
    private static function presco_measure_02_fixFukuchan(array &$l): void
    {
        if (($l[7] ?? '') === '5477') {
            $l[13] = 'gs_kaitori-1_fukuchan_pre';
        }
    }

    /* ---------- 行フォーマット ---------- */
    private static function formatRow(array $line)
    {
        $steps = array_filter(
            get_class_methods(self::class),
            fn($m) => str_starts_with($m, 'presco_measure_')
        );
        natsort($steps);

        foreach ($steps as $step) {
            $res = self::{$step}($line);
            if ($res === false) return false;
        }

        $afidField = $line[1] ?? '';

        return [
            // --- DB カラムに合わせて構築 ---
            'order_id'   => $line[0]  ?? null,
            'afid'       => $afidField,
            'click_time' => self::toDate($line[2]  ?? null),
            'occur_time' => self::toDate($line[3]  ?? null),
            'fix_time'   => self::toDate($line[4]  ?? null),
            'site_name'  => $line[5]  ?? null,
            'pg_name'    => $line[6]  ?? null,
            'lpid'       => $line[7]  ?? null,
            'lpname'     => $line[8]  ?? null,
            'ad_id'      => self::toInt($line[9]  ?? null),
            'u_id'       => $line[10] ?? null,
            'device'     => $line[11] ?? null,
            'referer'    => $line[12] ?? null,
            'param_1'    => $line[13] ?? null,
            'param_2'    => $line[14] ?? null,
            'param_3'    => $line[15] ?? null,
            'sales'      => self::toInt($line[16] ?? null),
            'reward'     => self::toInt($line[17] ?? null),
            'statement'  => $line[18] ?? null,
            'clickid'    => self::extractClid($afidField) ?: self::extractTclickId($afidField),
        ];
    }

    /* ---------- メイン ---------- */
    public function handle(): int
    {
        $sourceDir  = storage_path('app/csv/Measure');
        // presco_sales.csv を presco_measure.csv 相当として扱う
        $csvPattern = "$sourceDir/*presco_measure*.csv";
        $headers    = ['ID'];

        foreach (glob($csvPattern) as $csv) {
            // SJIS → UTF-8 変換（BOM 保持）
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
                    DB::table('measure_presco')->insert($row);
                });

            unlink($tmp);
        }

        $this->info('Presco 計測データ取込完了');
        return Command::SUCCESS;
    }
}
