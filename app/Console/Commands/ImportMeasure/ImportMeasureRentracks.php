<?php

namespace App\Console\Commands\ImportMeasure;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportMeasureRentracks extends Command
{
    protected $signature   = 'import:measure-rentracks';
    protected $description = 'Rentracks 計測 CSV を measure_rentracks へ取込む';

    /* ---------- 共通ヘルパ ---------- */
    private static function toDate(?string $v): ?string
    {
        $v = trim($v ?? '');
        if ($v === '' || $v === '0000-00-00 00:00:00') {
            return null;
        }
        $ts = @strtotime(str_replace('/', '-', preg_replace('/（.*）/', '', $v)));
        return $ts === false ? null : date('Y-m-d H:i:s', $ts);
    }

    private static function toInt(?string $v): int
    {
        if ($v === null || $v === '') {
            return 0;
        }
        return (int) floor((float) str_replace(',', '', $v));
    }

    private static function tax(string $date): float
    {
        return 1.10;
    }

    /* ---------- Rentracks 固有ステップ ---------- */
    private static function rr_measure_01_excludeRows(array &$l): bool
    {
        foreach (['0019791421', '0018102595', '0007378731', '0007437540'] as $id) {
            if (strpos($l[2] ?? '', $id) !== false) {
                return false;
            }
        }
        return true;
    }

    private static function rr_measure_02_correct250701(array &$l): void
    {
        if (strpos($l[2] ?? '', '0019664114') !== false) {
            $l[10] = "ys_supplements-1_nonlieys_rentracks_TCLICK_table-upper-nonlieys-1_p_nmnsuplmixedys_CLID__UTMC_";
        }
    }

    private static function rr_measure_03_appendPlatformLabel(array &$l): void
    {
        if (strpos($l[10] ?? '', 'ys_') !== false) {
            $l[4] .= '（YS）';
        } elseif (strpos($l[10] ?? '', 'yda_') !== false) {
            $l[4] .= '（YDA）';
        }
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
        if (strpos($field, '_TCLICK_') === false) {
            return '';
        }
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
                return strpos($right, '_UTMC_') !== false
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
                [, $utm]   = explode('_UTMC_', $right, 2);
                return $utm;
            }
        }
        return '';
    }

    /* ---------- 行フォーマット ---------- */
    private static function formatRow(array $line)
    {
        $steps = array_filter(
            get_class_methods(self::class),
            fn($m) => str_starts_with($m, 'rr_measure_')
        );
        natsort($steps);
        foreach ($steps as $step) {
            $res = self::{$step}($line);
            if ($res === false) {
                return false;
            }
        }

        $measureId = self::extractMeasureId($line[10] ?? '');
        $tclickId  = self::extractTclickId($line[10] ?? '');
        $clid      = self::extractClid($line[10] ?? '');
        $utm       = self::extractUtmContent($line[10] ?? '');

        $salesValue = (float) str_replace(',', '', $line[5] ?? '0');
        $rawReward  = (string) ($line[6]  ?? '0');
        $rewardValue = (float) str_replace(',', '', $rawReward);
        if ($rewardValue !== 0.0) {
            $rewardValue = round($rewardValue / self::tax($line[9] ?? ''), 2);
        }

        return [
            'click_time'     => self::toDate($line[0]  ?? null),
            'occur_time'     => self::toDate($line[1]  ?? null),
            'order_id'       => $line[2]             ?? null,
            'ad_owner'       => $line[3]             ?? null,
            'pg_name'        => $line[4]             ?? null,
            'sales'          => $salesValue,
            'reward'         => $rewardValue,
            'statement'      => $line[7]             ?? null,
            'approval_period'=> $line[8]             ?? null,
            'fix_time'       => self::toDate($line[9]  ?? null),
            'note'           => $measureId           ?: null,
            'site_id'        => isset($line[11])     ? (int) $line[11] : null,
            'site_name'      => $line[12]            ?? null,
            'device'         => $line[13]            ?? null,
            'referer'        => $line[14]            ?? null,
            'user_agent'     => $line[15]            ?? null,
            'reject_reason'  => $line[16]            ?? null,
            'gclid'          => $clid                ?: null,
            'utm_content'    => $utm                 ?: null,
            'tclick'         => $tclickId            ?: null,
        ];
    }

    /* ---------- メイン ---------- */
    public function handle(): int
    {
        $sourceDir  = storage_path('app/csv/Measure');
        $pattern    = "$sourceDir/*rentracks*.csv";
        $headers    = ['クリック日時'];

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
                    DB::table('measure_rentracks')->insert($row);
                });

            unlink($tmp);
        }

        $this->info('Rentracks 計測データ取込完了');
        return Command::SUCCESS;
    }
}
