<?php

namespace App\Console\Commands\ImportMeasure;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportMeasureAfb extends Command
{
    protected $signature   = 'import:measure-afb';
    protected $description = 'Afb 計測 CSV を measure_afb へ取込む';

    private static function toDate(?string $v): ?string
    {
        $v = trim($v ?? '');
        if ($v === '' || $v === '0000-00-00 00:00:00') return null;
        $ts = @strtotime(str_replace('/', '-', $v));
        return $ts === false ? null : date('Y-m-d H:i:s', $ts);
    }

    private static function toInt(?string $v): int
    {
        if ($v === null || $v === '') return 0;
        return (int) floor((float) str_replace(',', '', $v));
    }

    private static function afb_measure_01_excludeRows(array &$l): bool
    {
        if (($l[1] ?? '') === '' && ($l[2] ?? '') === '') return false;
        return true;
    }

    private static function afb_measure_02_correctMeasureId(array &$l): void
    {
        if (($l[12] ?? '') === '58519279') $l[17] = 'gs_dogfood-1_mogu_afb';
        if (($l[12] ?? '') === '58066718') $l[17] = 'gs_ippan-1_randstad_afb';
        if (($l[12] ?? '') === '57973489') $l[17] = 'gs_dogfood-1_moguwan_afb';
    }

    private static function afb_measure_03_normalizeSiteNames(array &$l): void
    {
        $f = $l[17] ?? '';
        if (strpos($f, 'ys_dogfood-1') !== false || strpos($f, 'ys_inukosyu-1') !== false) {
            $l[7] .= '（yahoo）';
        }
        if (strpos($f, 'ys_dogfood-2') !== false || strpos($f, 'ys_yousan-2') !== false) {
            $l[7] .= '（yahoo2）';
        }
    }

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

    private static function formatRow(array $l)
    {
        $steps = array_filter(get_class_methods(self::class), fn($m) => str_starts_with($m, 'afb_measure_'));
        natsort($steps);
        foreach ($steps as $step) {
            $res = self::{$step}($l);
            if ($res === false) return false;
        }

        $measureId = self::extractMeasureId($l[17] ?? '');
        $tclickId  = self::extractTclickId($l[17] ?? '');
        $rewardValue = self::toInt($l[9] ?? null);

        return [
            'click_time'  => self::toDate($l[1]  ?? null),
            'occur_time'  => self::toDate($l[2]  ?? null),
            'fix_time'    => self::toDate($l[3]  ?? null),
            'pg_id'       => isset($l[4])  ? (int)$l[4] : null,
            'pg_name'     => $l[5]         ?? null,
            'site_id'     => isset($l[6])  ? (int)$l[6] : null,
            'site_name'   => $l[7]         ?? null,
            'site_url'    => $l[8]         ?? null,
            'reward'      => $rewardValue,
            'statement'   => $l[10]        ?? null,
            'pb_id'       => $l[11]        ?? null,
            'order_id'    => isset($l[12]) ? (int)$l[12] : null,
            'sales'       => isset($l[13]) ? (int)$l[13] : null,
            'ad_id'       => isset($l[14]) ? (int)$l[14] : null,
            'device'      => $l[15]        ?? null,
            'referer'    => $l[16]        ?? null,
            'keyword'     => $measureId     ?: null,
            'useragent'   => $l[18]        ?? null,
            'tclick'      => $tclickId     ?: null,
        ];
    }

    public function handle(): int
    {
        $sourceDir  = storage_path('app/csv/Measure');
        $pattern    = "$sourceDir/*afb*.csv";
        $headers    = ['番号'];

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
                ->reject(fn($r) => empty($r) || in_array($r[0] ?? '', $headers, true))
                ->each(function(array $r) {
                    $row = self::formatRow($r);
                    if ($row === false) return;
                    DB::table('measure_afb')->insert($row);
                });

            unlink($tmp);
        }

        $this->info('Afb 計測データ取込完了');
        return Command::SUCCESS;
    }
}
