<?php

namespace App\Console\Commands\ImportMeasure;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportMeasureMp extends Command
{
    protected $signature   = 'import:measure-mp';
    protected $description = 'Mp 計測 CSV を measure_mp へ取込む';

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

    private static function tax(string $date): float
    {
        return 1.10; // 2025 年時点の標準税率
    }

    /* ---------- MP 固有ステップ ---------- */

    private static function mp_measure_01_trimLastColumn(array &$l): void
    {
        array_pop($l);
    }

    private static function mp_measure_02_excludeRows(array &$l): bool
    {
        $excludeClick = [
            '2025/07/18 17:00:52', '2025/07/18 10:24:42', '2025/07/17 17:31:26',
            '2025/07/08 10:20:13', '2025/07/08 10:20:23', '2025/07/08 10:25:59',
            '2025/04/10 00:36:15', '2025/07/14 11:46:48',
        ];
        $excludeOccur = [
            '2025-07-16 10:23:34', '2025-07-15 18:46:10', '2025-07-15 08:43:48',
            '2025-07-15 08:03:19', '2025-07-14 17:51:03', '2025-07-13 10:24:20',
            '2025-07-12 23:53:32', '2025-07-11 15:03:20', '2025-07-09 22:22:37',
            '2025-07-09 08:14:30', '2025-07-07 11:44:08', '2025-07-05 20:54:31',
            '2025-07-05 16:32:07', '2025-07-05 02:46:03', '2025-07-04 17:22:12',
            '2025-05-29 17:59:14', '2025-05-28 15:11:56',
        ];
        if (in_array($l[3] ?? '', $excludeClick, true)) return false;
        if (in_array($l[4] ?? '', $excludeOccur, true)) return false;
        if (($l[3] ?? '') === '2025/07/08 10:20:13' && ($l[5] ?? '') === '2025/07/08 11:02:56') return false;
        if (($l[3] ?? '') === '2025/07/08 10:20:23' && ($l[5] ?? '') === '2025/07/08 11:03:19') return false;
        if (($l[3] ?? '') === '2025/07/08 10:25:59' && ($l[5] ?? '') === '2025/07/08 11:03:29') return false;
        return true;
    }

    private static function mp_measure_03_cartBugFix(array &$l): void
    {
        if (($l[3] ?? '') === '2019/01/09 16:52:35' && ($l[1] ?? '') === 'ドッグフード/株式会社ミシュワン/ミシュワン') {
            static $cnt = 0;
            $cnt++;
            $l[8]  = 'gs_dogfood-1_mishone_mp';
            $l[12] = '転職エージェント評判｜-BEST WORK-';
            $l[4]  = '2025-06-03 10:00:00';
            if ($cnt <= 20) $l[4] = '2025-06-02 10:00:00';
            if ($cnt <= 10) $l[4] = '2025-06-01 10:00:00';
        }
    }

    private static function mp_measure_04_correctMeasureId_20250701(array &$l): void
    {
        if (($l[4] ?? '') === '2025-07-01 14:23:42') {
            $l[8] = 'gs_dogfood-1_mishone_mp_TCLICK_table-upper-mishone-3_p_dogfoodsougoumishone_CLID_Cj0KCQjwgIXCBhDBARIsAELC9Zjk74FV_X1VE0EZxG0LwFTrQKZJsP2lznu-wVJmLi2z0tz9nDvBg9QaAuVcEALw_wcB_UTMC_179773046545';
        }
    }

    private static function mp_measure_05_completeMotaMeasure(array &$l): void
    {
        if (($l[1] ?? '') === '車査定/株式会社MOTA/MOTA車査定' && ($l[12] ?? '') === 'MOTA_Meta' && ($l[8] ?? '') === '') {
            $l[8] = 'fb_usedcar-1_mota_mp';
        }
    }

    private static function mp_measure_06_choiceBetterFix(array &$l): void
    {
        if (($l[3] ?? '') === '2025/04/10 00:36:15' && ($l[4] ?? '') === '2025-04-10 00:42:22') {
            $l[8] = 'ys_yousan-2_beltamaka-ninkatsu_mp_TCLICK__CLID_YSS.EAIaIQobChMIuabauqLLjAMVk9oWBR25vBUaEAAYASAAEgLBFvD_BwE_UTMC_178797256281';
        }
    }

    private static function mp_measure_07_normalizeSiteNames(array &$l): void
    {
        if (($l[12] ?? '') === 'YS_葉酸サプリ-2') {
            $l[12] = '葉酸サプリ安心ランキング';
        }
        if (($l[12] ?? '') === 'ミシュワン_消化LP' || ($l[12] ?? '') === 'ミシュワン_アレルギーLP') {
            $l[12] = 'ミシュワン_通常';
        }
        if (strpos($l[8] ?? '', 'ys_dogfood-1') !== false) {
            $l[12] .= '（yahoo）';
        } elseif (strpos($l[8] ?? '', 'gdg_dogfood-1') !== false) {
            $l[12] .= '（gdg）';
        } elseif (strpos($l[8] ?? '', 'gs_dogfood-2') !== false) {
            $l[12] .= '（google2）';
        } elseif (strpos($l[8] ?? '', 'ms_dogfood-1') !== false) {
            if ($l[12] === 'ミシュワン_通常') $l[12] = 'ミシュワン_マイクロソフト';
            $l[12] .= '（microsoft）';
        } elseif (strpos($l[8] ?? '', 'ys_dogfood-2') !== false || strpos($l[8] ?? '', 'ys_yousan-2') !== false) {
            $l[12] .= '（yahoo2）';
        } elseif (strpos($l[8] ?? '', 'fb_dogfood-2') !== false && $l[12] !== 'FB_ドッグフード-2（SB）') {
            $l[12] .= '（比較FB）';
        }
        if (strpos($l[8] ?? '', 'mishone_mp') !== false && ($l[12] ?? '') === 'ミシュワン_通常') {
            $l[12] = '転職エージェント評判｜-BEST WORK-';
        }
        if (($l[12] ?? '') === '葉酸サプリ安心ランキング' && ($l[1] ?? '') === 'サプリ/妊活・葉酸/natural tech株式会社/mitas') {
            $l[12] = 'mitas';
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



    /* ---------- 行フォーマット ---------- */
    private static function formatRow(array $line)
    {
        $steps = array_filter(
            get_class_methods(self::class),
            fn($m) => str_starts_with($m, 'mp_measure_')
        );
        natsort($steps);

        foreach ($steps as $step) {
            $res = self::{$step}($line); // 動的呼び出し
            if ($res === false) return false;
        }

        $measureId = self::extractMeasureId($line[8] ?? '');
        $tclickId  = self::extractTclickId($line[8] ?? '');
        $clid      = self::extractClid($line[8] ?? '');
        $utm       = self::extractUtmContent($line[8] ?? '');

        $rewardValue = self::toInt($line[2] ?? null);
        if ($rewardValue !== 0 && ($line[5] ?? '') !== '') {
            $rewardValue = (int) round($rewardValue / self::tax($line[5]), 2);
        }

        return [
            'archive_type' => $line[0]  ?? null,
            'pg_name'      => ($line[1]  ?? '') . ($line[12] ?? ''),
            'reward'       => $rewardValue,
            'click_time'   => self::toDate($line[3]  ?? null),
            'occur_time'   => self::toDate($line[4]  ?? null),
            'fix_time'     => self::toDate($line[5]  ?? null),
            'statement'    => $line[7]  ?? null,
            'user_id'      => $measureId ?: null,
            'gclid'        => $clid      ?: null,
            'utm_content'  => $utm       ?: null,
            'device'       => $line[9]  ?? null,
            'referer'      => $line[10] ?? null,
            'keyword'      => $line[11] ?? null,
            'site_name'    => $line[12] ?? null,
            'tclick'       => $tclickId ?: null,
        ];
    }


    /* ---------- メイン ---------- */
    public function handle(): int
    {
        $sourceDir  = storage_path('app/csv/Measure');
        $csvPattern = "$sourceDir/*mp*.csv";
        $headers    = ['タイプ'];

        foreach (glob($csvPattern) as $csv) {
            // SJIS → UTF-8 変換（BOM 保護）
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

                    DB::table('measure_mp')->insert($row);
                });

            unlink($tmp);
        }

        $this->info('Mp 計測データ取込完了');
        return Command::SUCCESS;
    }
}
