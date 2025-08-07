<?php

namespace App\Console\Commands\ImportMeasure;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportMeasureFelmat extends Command
{
    protected $signature   = 'import:measure-felmat';
    protected $description = 'Felmat 計測 CSV を measure_felmat へ取込む';

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

    /* ---------- Felmat 固有ステップ ---------- */
    private static function felmat_measure_01_isBlank(array $l): bool
    {
        // 行頭が制御文字のみの場合はスキップ
        return !(($l[0] ?? '') === "\x1A");
    }

    private static function felmat_measure_02_excludeTestCv(array $l): bool
    {
        // テスト CV は除外
        return ($l[0] ?? '') !== '7593687';
    }

    private static function felmat_measure_03_correctId_0918(array &$l): void
    {
        if (($l[0] ?? 0) >= 9066385 &&
            strpos($l[16] ?? '', 'tt_saimu-1_mitsuba-4_set_cr') !== false
        ) {
            $l[16] = 'tt_saimu-1_mitsuba_fel?utm_source=tiktok?utm_creative=tt_saimu-1_mitsuba-3_set_cr';
        }
    }

    private static function felmat_measure_04_correctId_0830(array &$l): void
    {
        $map = [
            '8996158' => 'ys_dogfood-2_mishone_fel_YCLID_YSS.EAIaIQobChMIz4OwvOOZiAMVZ_9MAh1H2AvGEAAYASAAEgJ7-fD_BwE_UTMC_156212651719',
            '8966808' => 'ys_dogfood-2_mishone_fel_YCLID_YSS.EAIaIQobChMIrZne5f-FiAMVYQh7Bx1DUwbwEAAYASAAEgLlI_D_BwE_UTMC_162205086852',
            '8958189' => 'ys_dogfood-2_mishone_fel_YCLID_YSS.EAIaIQobChMIlYC0z6mAiAMVxloPAh2-WQqDEAAYAyAAEgK92fD_BwE_UTMC_156212651719',
        ];
        if (isset($map[$l[0] ?? ''])) {
            $l[16] = $map[$l[0]];
        }
    }

    private static function felmat_measure_05_trimQuery(array &$l): void
    {
        $targets = [
            'tt_saimu-1_tsuchiguri_fel?',
            'tt_saimu-1_rise_fel?',
            'tt_saimu-1_hibiki_fel?',
            'tt_saimu-1_mitsuba_fel?',
        ];
        foreach ($targets as $t) {
            if (strpos($l[16] ?? '', $t) !== false) {
                [$l[16]] = explode('?', $l[16], 2);
                break;
            }
        }
    }

    private static function felmat_measure_06_squadbeyondFix(array &$l): void
    {
        $m = [];
        if (preg_match('/[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}/', $l[16] ?? '', $m)) {
            // GUID を検出した場合
            $l[16] = 'unknown（' . ($l[1] ?? '') . '）';
            if (($l[4] ?? '') === '7507') {
                $l[16]   = 'gs_dogfood-2_mishone_fel';
                $l[14]   = 'ドッグフード2';
            }
        }
    }

    private static function felmat_measure_07_yahooSiteNameFix(array &$l): void
    {
        $mid = $l[16] ?? '';
        if (strpos($mid, 'ys_dogfood-1') !== false) {
            $l[14] = 'ドッグフード（yahoo）';
        } elseif (strpos($mid, 'ys_dogfood-2') !== false) {
            $l[14] = 'ドッグフード（yahoo2）';
        }
    }


    /* ---------- 分割ユーティリティ ---------- */
    private static function splitIds(string $measureField, string $utmcField): array
    {
        $measureId = $tclickId = $gclid = $utm = '';

        // ----- ① delimiter 一覧を配列化 -----
        $delimiters = ['_YCLID_', '_GCLID_', '_CLID_'];
        $found      = null;
        foreach ($delimiters as $d) {
            if (strpos($measureField, $d) !== false) {
                $found = $d;
                break;
            }
        }

        // ----- ② delimiter あり／なしで分岐 -----
        if ($found !== null) {
            [$left, $right] = explode($found, $measureField, 2);
            if (strpos($left, '_TCLICK_') !== false) {
                [$measureId, $tclickId] = explode('_TCLICK_', $left, 2);
            } else {
                $measureId = $left;
            }
            $gclid = $right;
        } else {
            $measureId = $measureField;
            if (strpos($measureId, '_TCLICK_') !== false) {
                [$measureId, $tclickId] = explode('_TCLICK_', $measureId, 2);
            }
            $gclid = $utmcField;  // param_1 が空の場合はこちら
        }

        // ----- ③ _UTMC_ 分離 -----
        if (strpos($gclid, '_UTMC_') !== false) {
            [$gclid, $utm] = explode('_UTMC_', $gclid, 2);
        }
        return [$measureId, $tclickId, $gclid, $utm];
    }



    /* ---------- 行フォーマット ---------- */
    private static function formatRow(array $line)
    {
        // Felmat 用ステップを自動収集
        $steps = array_filter(
            get_class_methods(self::class),
            fn($m) => str_starts_with($m, 'felmat_measure_')
        );
        natsort($steps); // 01,02… の順に実行

        foreach ($steps as $step) {
            $res = self::$step($line);    // $line は参照渡し
            if ($res === false) return false; // スキップ判定
        }

        [$measureId, $tclickId, $gclid, $utm] =
            self::splitIds($line[1] ?? '', $line[13] ?? '');

        return [
            'order_id'         => (int) ($line[0] ?? 0),
            'click_time'       => self::toDate($line[1]  ?? null),
            'occur_time'       => self::toDate($line[2]  ?? null),
            'fix_time'         => self::toDate($line[3]  ?? null),
            'pg_id'            => ($line[4]  ?? '') !== '' ? (int) $line[4] : null,
            'pg_name'          => $line[5]  ?? null,
            'ad_type'          => $line[6]  ?? null,
            'ad_id'            => $line[7]  ?? null,
            'ad_picsize'       => $line[8]  ?? null,
            'reward'           => self::toInt($line[9]  ?? null),
            'statement'        => $line[10] ?? null,
            'device'           => $line[11] ?? null,
            'os'               => $line[12] ?? null,
            'site_id'          => ($line[13] ?? '') !== '' ? (int) $line[13] : null,
            'site_name'        => $line[14] ?? null,
            'referer'          => $line[15] ?? null,
            'param_1'          => $line[16] ?? null,
            'is_external'      => $line[17] ?? null,
            'search_engine'    => $line[18] ?? null,
            'search_word'      => $line[19] ?? null,
            'search_lpid'      => $line[20] ?? null,
            'contracted_state' => $line[21] ?? null,
            'reward_id'        => $line[22] ?? null,
            'reward_name'      => $line[23] ?? null,
            'tclick'           => $tclickId ?: null,
        ];
    }

    /* ---------- メイン ---------- */
    public function handle(): int
    {
        $sourceDir  = storage_path('app/csv/Measure');
        $csvPattern = "$sourceDir/*felmat*.csv";
        $headers    = ['成果番号'];

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

                    DB::table('measure_felmat')->insert($row);
                });

            unlink($tmp);
        }

        $this->info('Felmat 計測データ取込完了');
        return Command::SUCCESS;
    }
}
